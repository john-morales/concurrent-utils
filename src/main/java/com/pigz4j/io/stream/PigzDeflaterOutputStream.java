package com.pigz4j.io.stream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * Used as underlying heavy-lifting of threaded GZIP-compatible deflating per block and re-sequencing.
 * Should not be used directly.
 *
 * Usage requires calling finish() before close().
 */
class PigzDeflaterOutputStream extends FilterOutputStream {

    static final int DEFAULT_BLOCK_SIZE = 1 << 17; // 128k
    static final Logger LOG = Logger.getLogger( PigzDeflaterOutputStream.class.getName() );

    private final ExecutorService _executorService;
    private final AtomicLong _sequencer;
    private final AtomicLong _totalIn;
    private final AtomicLong _totalOut;

    private final int _blockSize;
    private final IPigzDeflaterFactory _deflaterFactory;
    private final OutWriter _outWorker;

    protected GzipWorker _lastWorker;
    protected GzipWorker _previousWorker;

    private volatile boolean _isFinished;


    public PigzDeflaterOutputStream(final OutputStream pOut,
                                    final IPigzDeflaterFactory pDeflaterFactory,
                                    final ExecutorService pExecutorService) throws IOException {
        this(pOut, DEFAULT_BLOCK_SIZE, pDeflaterFactory, pExecutorService);
    }

    public PigzDeflaterOutputStream(final OutputStream pOut, final int pBlockSize,
                                    final IPigzDeflaterFactory pDeflaterFactory,
                                    final ExecutorService pExecutorService) throws IOException {
        super(pOut);
        if ( pOut == null ) {
            throw new NullPointerException("null underlying output stream");
        }
        if ( pDeflaterFactory == null ) {
            throw new NullPointerException("null deflater factory");
        }
        if ( pExecutorService == null ) {
            throw new NullPointerException("null executor service");
        }
        if ( pExecutorService.isShutdown() ) {
            throw new IllegalArgumentException("executor service has been shutdown");
        }
        if ( pBlockSize < 1 ) {
            throw new IllegalArgumentException("block size < 1: " + pBlockSize);
        }

        _deflaterFactory = pDeflaterFactory;
        _executorService = pExecutorService;
        _isFinished = false;
        _blockSize = pBlockSize;
        _lastWorker = _previousWorker = null;

        _sequencer = new AtomicLong(0L);
        _totalIn = new AtomicLong(0L);
        _totalOut = new AtomicLong(0L);

        _outWorker = new OutWriter(pOut);
        _outWorker.start();
    }

    private void _finish(final long pTime, final TimeUnit pUnit) throws IOException {
        try {
            _outWorker.finish();
            _outWorker.await(pTime, pUnit);
        } catch (InterruptedException e) {
            LOG.log(Level.WARNING, "_finish: interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // don't care - called in finally, don't want to hide higher exception
            LOG.log(Level.WARNING, "_finish: unexpected failure", e);
        }
    }

    /**
     * Blocks until gzip worker threads and out writer thread have finished.
     * If errors occurred in both gzip workers and out writer, worker errors
     * take precedence.
     */
    public void finish(final long pTime, final TimeUnit pUnit) throws IOException {
        // TODO: make synchronized?
        if ( !_isFinished) {
            try {
                // quick check now for errors before attempting to wait on workers.
                assertNoError();

                if ( _lastWorker != null ) {
                    if ( !_lastWorker.awaitDone(pTime, pUnit) ) {
                        throw new IllegalStateException("executor service did not shutdown within timeout: " + pUnit.toMillis(pTime) + "msec");
                    }
                }
            } catch (Exception e) {
                _outWorker.cancel();
                throw new IOException("error during stream finish", e);
            } finally {
                _finish(pTime, pUnit);
            }

            // After everything shut down, assert no late errors from out writer.
            assertNoError();
            _isFinished = true;
            LOG.log(Level.FINE, "finish: stream finished successfully");
        }
    }

    /**
     * Not thread safe - multiple threads should not share a PigzOutputStream instance.
     * @throws java.io.IOException
     */
    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        // TODO: make synchronized?
        assertNoError();

        final byte[] payload = Arrays.copyOfRange(b, off, off + len);
        int offset = 0;
        int remaining = len;
        if (LOG.isLoggable( Level.FINE )) { LOG.log(Level.FINE, "writing offset=" + off + " length=" + len); }

        while ( remaining > 0 ) {
            final int length = Math.min(_blockSize, remaining);

            final GzipWorker worker = newWorker(_sequencer.getAndIncrement(), payload, offset, length);
            submitWorker(worker);
            if (LOG.isLoggable( Level.FINEST )) { LOG.log(Level.FINEST, "Worker submitted with offset=" + offset + " length=" + length); }

            remaining -= length;
            offset += length;
        }
    }

    /**
     * Calls 3 argument write() method.
     * Not thread safe - multiple threads should not share a PigzOutputStream instance.
     * @throws java.io.IOException
     */
    @Override
    public void write(int b) throws IOException {
        byte[] buf = new byte[1];
        buf[0] = (byte)(b & 0xff);
        write(buf, 0, 1);
    }

    /**
     * Optionally shutdown executor service before closing underlying stream.
     *
     * Important to note that if this stream was created with the default executor
     * service, then care should be taken if instructing it to be shutdown. Doing
     * so will cause any future creations of PigzDeflaterOutputStream instances
     * to fail at construction time.
     *
     * @param pShutdown true if this stream's executor service should be shutdown.
     * @throws IOException
     */
    public void close(final boolean pShutdown) throws IOException {
        if ( !_isFinished) {
            throw new IllegalStateException("closed called without having finished");
        }

        if ( pShutdown ) {
            _executorService.shutdownNow();
        }
        LOG.log(Level.FINE, "close: stream closed successfully");
        super.close();
    }

    private void assertNoError() throws IOException {
        _outWorker.throwOnError();
        if ( _lastWorker != null ) {
            _lastWorker.throwOnError();
        }
    }

    /**
     * @return snapshot of total (uncompressed) bytes processed by the gzip workers
     * across PigzDeflater instances.
     */
    public long getTotalIn() {
        return _totalIn.get();
    }

    /**
     * @return snapshot of total (compressed) bytes outputted by gzip workers across
     * PigzDeflater instances.
     */
    public long getTotalOut() {
        return _totalOut.get();
    }

    /**
     * @return snapshot of current compression ratio observed across all gzip workers.
     */
    public double getCompressionRatio() {
        return getTotalOut() / (double)getTotalIn();
    }

    /**
     * @return deflater instance from factory
     */
    protected PigzDeflater getDeflater() {
        return _deflaterFactory.getDeflater();
    }

    /**
     * Submit gzip worker thread to executor service for processing the uncompressed input block.
     * @param pWorker
     * @return Future of submitted worker
     */
    protected Future<?> submitWorker(final GzipWorker pWorker) {
        if (_isFinished) {
            throw new IllegalStateException("received submit after stream already shutdown");
        }
        if ( pWorker == null ) {
            throw new IllegalStateException("submitted null worker");
        }

        _previousWorker = _lastWorker;
        _lastWorker = pWorker;
        return _executorService.submit(pWorker);
    }

    /**
     *
     * @param pSequence
     * @param b
     * @param pOff
     * @param pLen
     * @return new worker thread to process this uncompressed input block.
     * @throws IOException
     */
    protected GzipWorker newWorker(final long pSequence, final byte[] b, final int pOff, final int pLen) throws IOException {
        return new GzipWorker(pSequence, _outWorker, _lastWorker, _blockSize, b, pOff, pLen);
    }

    /**
     * Optional hook to be notified on completion of a new gzipped block, but before
     * it's been submitted to the out writer for writing to the underlying stream.
     * *NOT* guaranteed to be called in input sequence order.
     * @param pBlock single complete gzip-compatible block.
     */
    protected void onBlock(final GzipBlock pBlock) {
        _totalIn.addAndGet(pBlock.blockIn());
        _totalOut.addAndGet(pBlock.blockOut());
    }

    protected boolean isDefaultExecutor() {
        return _executorService == PigzOutputStream.getDefaultExecutorService();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        _outWorker.cancel();
        if ( !isDefaultExecutor() ) {
            LOG.log(Level.FINE, "finalize: Shutting down executor...");
            _executorService.shutdownNow();
            LOG.log(Level.FINE, "finalize: Executor shutdown");
        }
    }

    class GzipWorker implements Runnable {

        private final long _sequence;
        private final OutWriter _outWorker;
        private final GzipWorker _previous;
        private final CountDownLatch _doneLatch;
        private final int _blockSize;

        private final byte[] _buffer;
        private final int _offset;
        private final int _len;

        private IOException _thrown;

        public GzipWorker(final long pSequence, final OutWriter pOutWorker, final GzipWorker pPrevious, final int pBlockSize,
                          final byte[] pBuf, final int pOff, final int pLen) throws IOException {
            _sequence = pSequence;
            _outWorker = pOutWorker;
            _previous = pPrevious;
            _blockSize = pBlockSize;
            _doneLatch = new CountDownLatch(1);

            _buffer = pBuf;
            _offset = pOff;
            _len = pLen;
        }

        boolean awaitDone() throws IOException {
            return awaitDone(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }

        boolean awaitDone(final long pV, final TimeUnit pUnit) throws IOException {
            try {
                return _doneLatch.await(pV, pUnit);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        void done() {
            LOG.log(Level.FINEST, "gzip: done");
            _doneLatch.countDown();
        }

        private void awaitPreviousDone() throws IOException {
            if ( _previous != null ) {
                _previous.awaitDone();
            }
        }

        void throwOnError() throws IOException {
            _outWorker.throwOnError();
            if ( _thrown != null ) {
                throw new IOException("GzipWorker failure", _thrown);
            }
        }

        public void run() {
            try {

                // TODO: investigate prepopulating deflate dictionary ala pigz.
                final GzipBlock blockDelegate = new GzipBlock(getDeflater(), _blockSize);
                blockDelegate.write(_buffer, _offset, _len);
                blockDelegate.finish();
                onBlock(blockDelegate);

                awaitPreviousDone();
                // NOTE: effectively starts a critical section. Provides in-order writing.

                _outWorker.enqueueBlock(blockDelegate);
            } catch (IOException e) {
                _thrown = e;
                throw new IllegalStateException(e);
            } catch (Exception e) {
                _thrown = new IOException(e);
                throw new IllegalStateException(e);
            } finally {
                // NOTE: effectively ends critical section, enabling next block in sequence.
                done();
            }
        }

    }

    private static class GzipBlock extends DeflaterOutputStream {

        private final CRC32 _blockCrc;
        private final ByteArrayOutputStream _underlyingStream;
        private final Deflater _underlyingDeflater;

        public GzipBlock(final Deflater deflater, int pBlockSize) throws IOException {
            this(new ByteArrayOutputStream(pBlockSize), deflater, pBlockSize);
        }

        private GzipBlock(final ByteArrayOutputStream out, final Deflater deflater, final int pBlockSize) throws IOException {
            super(out, deflater, pBlockSize);
            _underlyingStream = out;
            _underlyingDeflater = deflater;
            _blockCrc = new CRC32();

            _underlyingStream.write(GZIP_HEADER);
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            super.write(b, off, len);
            _blockCrc.update(b, off, len);
        }

        void writeTo(final OutputStream pOut) throws IOException {
            _underlyingStream.writeTo(pOut);
        }

        @Override
        public void finish() throws IOException {
            super.finish();
            _underlyingStream.write(trailer());
            LOG.log(Level.FINEST, "gzip: block finish");
        }

        byte[] trailer() throws IOException {
            final int crcValue = (int) _blockCrc.getValue();
            final int totalIn = _underlyingDeflater.getTotalIn();

            final ByteBuffer trailer = ByteBuffer.allocate(8);
            trailer.order(ByteOrder.LITTLE_ENDIAN);
            trailer.putInt(crcValue);
            trailer.putInt(totalIn);

            return trailer.array();
        }

        int blockIn() {
            return _underlyingDeflater.getTotalIn();
        }

        int blockOut() {
            return _underlyingDeflater.getTotalOut();
        }

        /**
         * GZIP Spec with no modification time, extra flags or OS info.
         */
        private final static byte[] GZIP_HEADER = {31, -117, 8, 0, 0, 0, 0, 0, 0, 0};
    }

    private static class OutWriter extends Thread {
        private static final AtomicInteger SERIAL = new AtomicInteger(0);
        private static final String _namePrefix = "pigz4j-OutWorker-";

        private final BlockingQueue<GzipBlock> _blockQueue;
        private final CountDownLatch _doneLatch;
        private final OutputStream _out;

        private IOException _thrown;
        private volatile boolean _isCancelled;

        OutWriter(final OutputStream pOut) {
            super(_namePrefix + SERIAL.getAndIncrement());
            setDaemon(true);

            _out = pOut;
            _blockQueue = new LinkedBlockingQueue<GzipBlock>();
            _doneLatch = new CountDownLatch(1);
            _isCancelled = false;
        }

        private void write(final GzipBlock pBlock) {
            try {
                pBlock.writeTo(_out);
            } catch (IOException e) {
                _thrown = e;
            } catch (Exception e) {
                _thrown = new IOException(e);
            }
        }

        void throwOnError() throws IOException {
            if ( _thrown != null ) {
                throw new IOException("OutWriter failure", _thrown);
            }
        }

        void enqueueBlock(final GzipBlock pBlock) throws InterruptedException, IOException {
            throwOnError();
            _blockQueue.put(pBlock);
        }

        void await() throws InterruptedException {
            _doneLatch.await();
        }

        boolean await(final long pV, final TimeUnit pUnit) throws InterruptedException {
            return _doneLatch.await(pV, pUnit);
        }

        private void drainQueue() {
            GzipBlock block;
            try {
                while ( (block = _blockQueue.take()) != null ) {
                    write(block);
                }
            } catch (final InterruptedException pE) {
                interrupt();
                if ( _isCancelled ) { return; }
            }

            while ( (block = _blockQueue.poll()) != null ) {
                write(block);
            }
        }

        @Override
        public void run() {
            try {
                drainQueue();
            } finally {
                _doneLatch.countDown();
            }
        }

        void finish() {
            interrupt();
            LOG.log(Level.FINE, "out: finished");
        }

        void cancel() {
            _isCancelled = true;
            finish();
            LOG.log(Level.FINE, "out: cancelled");
        }
    }

}

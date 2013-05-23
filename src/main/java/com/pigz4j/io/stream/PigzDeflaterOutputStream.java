package com.pigz4j.io.stream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

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
    private final GzipWriter _outWriter;
    private final AtomicReference<IOException> _writeError;
    private final CRC32 _crc;

    private GzipWorker _lastWorker;
    private boolean _isFinished;
    private boolean _isClosed;


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
        _blockSize = pBlockSize;
        _isFinished = _isClosed = false;
        _lastWorker = null;
        _writeError = new AtomicReference<IOException>(null);

        _sequencer = new AtomicLong(0L);
        _totalIn = new AtomicLong(0L);
        _totalOut = new AtomicLong(0L);
        _crc = new CRC32();

        pOut.write(GZIP_HEADER);
        _outWriter = new GzipWriter(pOut);
        _outWriter.start();
    }

    /**
     * Blocks until gzip worker threads and out writer thread have finished.
     * If errors occurred in both gzip workers and out writer, worker errors
     * take precedence.
     */
    public void finish(final long pTime, final TimeUnit pUnit) throws IOException {
        if ( !_isFinished) {
            try {
                // quick check now for errors before attempting to wait on workers.
                assertNoError();

                if ( _lastWorker != null ) {
                    if ( !_lastWorker.await(pTime, pUnit) ) {
                        throw new IOException("finish: last gzip worker thread did not complete within " + pUnit.toMillis(pTime) + "msec");
                    }
                }
                _outWriter.finish();
            } catch (Exception e) {
                _outWriter.cancel();
                throw new IOException("finish: error during stream finish", e);
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
    public void write(final byte[] pBuf, final int pOff, final int pLen) throws IOException {
        assertNoError();

        final byte[] block = Arrays.copyOfRange(pBuf, pOff, pOff + pLen);
        LOG.log(Level.FINE, "write: writing {0} bytes total", pLen);

        int remaining = pLen;
        int blockOffset = 0;
        while ( remaining > 0 ) {
            final int blockLen = Math.min(_blockSize, remaining);

            submit(block, blockOffset, blockLen);
            LOG.log(Level.FINEST, "write: worker submitted with block of {0} bytes", blockLen);

            remaining -= blockLen;
            blockOffset += blockLen;
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


    protected byte[] trailer() {
        final int crcValue = (int) _crc.getValue();
        final int isize = (int) getTotalIn();

        final ByteBuffer trailer = ByteBuffer.allocate(8);
        trailer.order(ByteOrder.LITTLE_ENDIAN);
        trailer.putInt(crcValue);
        trailer.putInt(isize);

        return trailer.array();
    }

    private void _close() throws IOException {
        _outWriter.await();

        // Writing closing bytes to stream - does not have to be same deflater
        final byte[] buf = new byte[_blockSize];
        final PigzDeflater def = getDeflater();
        def.finish();
        while ( !def.finished() ) {
            int deflated = def.deflate(buf);
            if ( deflated > 0 ) { out.write(buf, 0, deflated); }
        }
        out.write(trailer());
    }

    @Override
    public void close() throws IOException {
        this.close(!isDefaultExecutor());
    }

    /**
     * Optionally shutdown executor service before closing underlying stream.
     *
     * Important to note that if this stream was created with the default executor
     * service, then care should be taken if instructing it to be shutdown. Doing
     * so will cause any future creations of PigzDeflaterOutputStream instances
     * to fail at construction time.
     *
     * @throws IOException
     */
    public void close(final boolean pShutdown) throws IOException {
        if ( !_isFinished) {
            throw new IllegalStateException("bug: close called without having called finish");
        }
        if ( pShutdown ) {
            _executorService.shutdownNow();
        }

        _close();
        super.close();
        _isClosed = true;

        assertNoError();
        LOG.log(Level.FINE, "close: stream closed successfully");
    }

    private void assertNoError() throws IOException {
        _outWriter.throwOnError();
        final IOException ioe = _writeError.get();
        if ( ioe != null ) { throw ioe; }
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
     * Submit sequenced slice of bytes executor service for processing the uncompressed input block.
     * @return Future of submitted worker
     */
    protected Future<?> submit(final byte[] pBuf, final int pOff, final int pLen) {
        if (_isFinished) {
            throw new IllegalStateException("received submit after stream already shutdown");
        }

        _lastWorker = new GzipWorker(this, _sequencer.getAndIncrement(), _lastWorker, pBuf, pOff, pLen);
        return _executorService.submit(_lastWorker);
    }

    /**
     * Optional hook to be notified on completion of a new gzipped block, but before
     * it's been submitted for writing to the underlying stream.
     * @param pBlock
     */
    protected void onBlockUnsequenced(final GzipBlock pBlock) {
        _totalIn.addAndGet(pBlock.blockIn());
        _totalOut.addAndGet(pBlock.blockOut());
    }

    /**
     * Optional hook to be notified in sequence order on completion of a new gzipped
     * block, but before it's been submitted for writing ot underlying stream.
     * @param pWorker
     * @param pBlock
     */
    protected void onBlockSequenced(final GzipWorker pWorker, final GzipBlock pBlock) throws IOException, InterruptedException {
        _crc.update(pWorker._buffer, pWorker._offset, pWorker._len);
        _outWriter.enqueueBlock(pBlock);
    }

    protected boolean isDefaultExecutor() {
        return _executorService == PigzOutputStream.getDefaultExecutorService();
    }

    protected boolean setWriteError(final IOException expect, final IOException update) {
        return _writeError.compareAndSet(expect, update);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();

        if ( !_isClosed ) {
            LOG.log(Level.WARNING, "finalize: GC cleaning up; stream was never closed");
            _outWriter.cancel();
            if ( !isDefaultExecutor() ) {
                LOG.log(Level.FINE, "finalize: Shutting down executor...");
                _executorService.shutdownNow();
                LOG.log(Level.FINE, "finalize: Executor shutdown");
            }
        }
    }

    private static class GzipWorker implements Runnable {

        private final PigzDeflaterOutputStream _stream;
        private final long _sequence;
        private final GzipWorker _previous;
        private final CountDownLatch _doneLatch;

        private final byte[] _buffer;
        private final int _offset;
        private final int _len;

        public GzipWorker(final PigzDeflaterOutputStream pStream, final long pSequence, final GzipWorker pPrevious,
                          final byte[] pBuf, final int pOff, final int pLen) {
            _stream = pStream;
            _sequence = pSequence;
            _previous = pPrevious;
            _doneLatch = new CountDownLatch(1);

            _buffer = pBuf;
            _offset = pOff;
            _len = pLen;
        }

        boolean await() throws IOException {
            return await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }

        boolean await(final long pV, final TimeUnit pUnit) throws IOException {
            try {
                return _doneLatch.await(pV, pUnit);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        void done() {
            LOG.log(Level.FINEST, "worker: sequence={0} done", _sequence);
            _doneLatch.countDown();
        }

        private void awaitPrevious() throws IOException {
            if ( _previous != null ) {
                _previous.await();
            }
        }

        public void run() {
            try {
                LOG.log(Level.FINEST, "worker: sequence={0} start", _sequence);

                final GzipBlock blockDelegate = new GzipBlock(_stream.getDeflater(), _stream._blockSize);
                blockDelegate.setDictionary(_previous);
                blockDelegate.write(_buffer, _offset, _len);
                blockDelegate.finish();
                _stream.onBlockUnsequenced(blockDelegate);

                awaitPrevious();
                // NOTE: effectively starts a critical section. Provides in-order writing.

                _stream.onBlockSequenced(this, blockDelegate);
            } catch (IOException e) {
                LOG.log(Level.SEVERE, "worker: sequence=" + _sequence + " in illegal i/o state", e);
                _stream.setWriteError(null, e);
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "worker: sequence=" + _sequence + " in illegal state", e);
                _stream.setWriteError(null, new IOException(e));
            } finally {
                // NOTE: effectively ends critical section, enabling next block in sequence.
                done();
            }
        }
    }

    private static class GzipBlock extends ByteArrayOutputStream {
        public static final GzipBlock SENTINEL = new GzipBlock();

        private final Deflater _deflater;
        private final byte[] _buffer;

        private GzipBlock() {
            this(null, 0);
        }

        public GzipBlock(final Deflater deflater, int pBlockSize) {
            super(pBlockSize);
            _deflater = deflater;
            _buffer = new byte[pBlockSize];
        }

        @Override
        public void write(final byte[] b, final int off, final int len) {
            if (len == 0) { return; }

            _deflater.setInput(b, off, len);
            int written = _deflater.deflate(_buffer, 0, _buffer.length, Deflater.NO_FLUSH);
            if (written > 0) { super.write(_buffer, 0, written); }
        }

        public void finish() {
            int written = _deflater.deflate(_buffer, 0, _buffer.length, Deflater.SYNC_FLUSH);
            if (written > 0) { super.write(_buffer, 0, written); }
            LOG.log(Level.FINEST, "gzip: block finish");
        }

        void setDictionary(final GzipWorker pPrevious) {
            if ( pPrevious != null ) {
                // prep dictionary with last 1/4 of previous block
                final int scaledLen = pPrevious._len / 4;
                final int scaledOffset = pPrevious._offset - scaledLen + pPrevious._len;
                _deflater.setDictionary(pPrevious._buffer, scaledOffset, scaledLen);
                LOG.log(Level.FINEST, "gzip: using previous trailing {0} bytes", scaledLen);
            }
        }

        int blockIn() {
            return _deflater.getTotalIn();
        }

        int blockOut() {
            return _deflater.getTotalOut();
        }
    }

    private static class GzipWriter extends Thread {
        private static final AtomicInteger SERIAL = new AtomicInteger(0);
        private static final String PREFIX = "pigz4j-OutWorker-";

        private final BlockingQueue<GzipBlock> _blockQueue;
        private final CountDownLatch _doneLatch;
        private final OutputStream _out;

        private IOException _thrown;

        GzipWriter(final OutputStream pOut) {
            super(PREFIX + SERIAL.getAndIncrement());
            setDaemon(true);

            _out = pOut;
            _blockQueue = new LinkedBlockingQueue<GzipBlock>();
            _doneLatch = new CountDownLatch(1);
        }

        void throwOnError() throws IOException {
            if ( _thrown != null ) { throw _thrown; }
        }

        void enqueueBlock(final GzipBlock pBlock) throws InterruptedException, IOException {
            throwOnError();
            _blockQueue.put(pBlock);
        }

        void await() throws IOException {
            try {
                _doneLatch.await();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void run() {
            try {
                GzipBlock block;
                while ( (block = _blockQueue.take()) != GzipBlock.SENTINEL ) {
                    block.writeTo(_out);
                }
            } catch (final InterruptedException e) {
                LOG.log(Level.FINE, "out: worker interrupted (cancelled); queued={0}", _blockQueue.size());
            } catch (IOException e) {
                _thrown = e;
                LOG.log(Level.SEVERE, "out: i/o error writing to underlying stream; queued=" + _blockQueue.size(), e);
            } catch (Exception e) {
                _thrown = new IOException(e);
                LOG.log(Level.SEVERE, "out: error writing to underlying stream; queued=" + _blockQueue.size(), e);
            } finally {
                _doneLatch.countDown();
            }
        }

        void finish() throws IOException {
            try {
                enqueueBlock(GzipBlock.SENTINEL);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        void cancel() {
            interrupt();
            LOG.log(Level.FINE, "out: cancelled");
        }
    }

    /**
     * GZIP Spec with no modification time, extra flags or OS info.
     */
    private final static byte[] GZIP_HEADER = {31, -117, 8, 0, 0, 0, 0, 0, 0, 0};

}

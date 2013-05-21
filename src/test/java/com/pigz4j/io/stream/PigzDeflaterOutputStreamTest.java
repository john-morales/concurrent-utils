package com.pigz4j.io.stream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PigzDeflaterOutputStreamTest {

    @BeforeClass
    public static void setUp() throws Exception {
        Logger.getLogger("com.pigz4j").setLevel(Level.OFF);
    }

    @Test public void assertInvariants() throws Exception {
        try {
            new PigzDeflaterOutputStream(null, PigzDeflaterFactory.DEFAULT, Executors.newSingleThreadExecutor());
            fail("expecting NPE from null output stream");
        } catch (NullPointerException expected) {
        }

        try {
            new PigzDeflaterOutputStream(new ByteArrayOutputStream(), null, Executors.newSingleThreadExecutor());
            fail("expecting NPE from null deflater factory");
        } catch (NullPointerException expected) {
        }

        try {
            new PigzDeflaterOutputStream(new ByteArrayOutputStream(), PigzDeflaterFactory.DEFAULT, null);
            fail("expecting NPE from null executor service");
        } catch (NullPointerException expected) {
        }

        try {
            new PigzDeflaterOutputStream(new ByteArrayOutputStream(), 0, PigzDeflaterFactory.DEFAULT, Executors.newSingleThreadExecutor());
            fail("expecting IAE from block size less than 1");
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test public void exceptionsThrown_header() throws Exception {
        try {
            // allow single write of the gzip header
            final PigzDeflaterOutputStream out = new PigzDeflaterOutputStream(new FlakyOutputStream(0),
                    PigzDeflaterFactory.DEFAULT,
                    Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                            PigzOutputStream.DAEMON_FACTORY));

            fail("expecting IOException on write");
        } catch (IOException expected) {
            assertEquals("Flake out!", expected.getMessage());
        }
    }

    @Test public void exceptionsThrown_write() throws Exception {
        try {
            // allow single write of the gzip header
            final FlakyOutputStream flake = new FlakyOutputStream(1);
            final PigzDeflaterOutputStream out = new PigzDeflaterOutputStream(flake,
                    PigzDeflaterFactory.DEFAULT,
                    Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                            PigzOutputStream.DAEMON_FACTORY));

            out.write(1);
            flake.throwLatch.await(10, TimeUnit.SECONDS);
            out.write(2);
            fail("expecting IOException on write");
        } catch (IOException expected) {
            assertEquals("Flake out!", expected.getMessage());
        }
    }

    @Test public void exceptionsRethrown_finish() throws Exception {
        try {
            // allow single write of the gzip header
            final FlakyOutputStream flake = new FlakyOutputStream(1);
            final PigzDeflaterOutputStream out = new PigzDeflaterOutputStream(flake,
                    PigzDeflaterFactory.DEFAULT,
                    Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                            PigzOutputStream.DAEMON_FACTORY));

            out.write(1);
            flake.throwLatch.await(10, TimeUnit.SECONDS);
            out.finish(1, TimeUnit.SECONDS);

            fail("expecting IOException on finish");
        } catch (IOException expected) {
            assertEquals("Flake out!", expected.getCause().getMessage());
        }
    }

    @Test public void timeoutsRethrown() throws Exception {
        try {
            final SleepyDeflaterFactory sleepyFactory = new SleepyDeflaterFactory();
            final PigzDeflaterOutputStream out = new PigzDeflaterOutputStream(new ByteArrayOutputStream(),
                    sleepyFactory,
                    Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                            PigzOutputStream.DAEMON_FACTORY));

            out.write(1);
            sleepyFactory.sleepLatch.await();
            out.finish(1, TimeUnit.MILLISECONDS);
            fail("expecting IOException on deflating timeout");
        } catch (IOException expected) {
            assertEquals("finish: last gzip worker thread did not complete within 1msec", expected.getCause().getMessage());
        }
    }

    private static class FlakyOutputStream extends OutputStream {

        private final CountDownLatch throwLatch;
        private int callsAllowed;

        FlakyOutputStream(final int pCallsAllowed) {
            callsAllowed = pCallsAllowed;
            throwLatch = new CountDownLatch(1);
        }

        @Override
        public void write(final int b) throws IOException {
            throw new IllegalStateException("CRITICAL: shouldn't ever be called!");
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            if ( --callsAllowed < 0 ) {
                try {
                    throw new IOException("Flake out!");
                } finally {
                    throwLatch.countDown();
                }
            }
        }
    }

    private static class SleepyDeflaterFactory implements IPigzDeflaterFactory {

        private final CountDownLatch sleepLatch;

        SleepyDeflaterFactory() {
            sleepLatch = new CountDownLatch(1);
        }

        public PigzDeflater getDeflater() {
            return new PigzDeflater() {
                @Override
                public int deflate(final byte[] b, final int off, final int len, final int flush) {
                    try {
                        sleepLatch.countDown();
                        Thread.sleep(2000);
                        return 0;
                    }
                    catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
            };
        }
    }

}

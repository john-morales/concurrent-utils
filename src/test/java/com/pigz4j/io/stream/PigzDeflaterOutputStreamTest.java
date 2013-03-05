package com.pigz4j.io.stream;

import com.pigz4j.io.stream.PigzDeflaterFactory;
import com.pigz4j.io.stream.PigzDeflaterOutputStream;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PigzDeflaterOutputStreamTest {

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

    @Test public void exceptionsRethrown() throws Exception {
        try {
            final PigzDeflaterOutputStream out = new PigzDeflaterOutputStream(new FlakyOutputStream(),
                    PigzDeflaterFactory.DEFAULT,
                    Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));

            out.write(1);
            out.flush();
            out.finish(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            out.close();
            fail("expecting IOException on finish");
        } catch (IOException expected) {
            assertTrue(expected.getMessage(), expected.getCause().getMessage().contains("Flake out!"));
        }
    }

    private static class FlakyOutputStream extends OutputStream {
        @Override
        public void write(final int b) throws IOException {
            throw new IllegalStateException("CRITICAL: shouldn't ever be called!");
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            throw new IOException("Flake out!");
        }
    }

}

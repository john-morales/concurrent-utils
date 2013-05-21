package com.pigz4j.io.stream;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertTrue;

public class PigzOutputStreamTest extends PigzTest {

    private static final int INPUT_SIZE = 2 * ONE_MB;

    @Test public void testRepeat() throws Exception {
        testCompression_Random();
        testCompression_Random();
    }

    @Test public void testCompression_Random() throws Exception {
        final byte[] sourceBytes = generateRandomBytes(INPUT_SIZE);

        final ByteArrayOutputStream compressed = new ByteArrayOutputStream();

        final PigzOutputStream out = new PigzOutputStream(compressed,
                PigzOutputStream.DEFAULT_BUFSZ,
                PigzDeflaterOutputStream.DEFAULT_BLOCK_SIZE);

        out.write(sourceBytes);
        out.close();

        final byte[] inflatedBytes = inflate(compressed).toByteArray();
        assertTrue(Arrays.equals(sourceBytes, inflatedBytes));
    }

    @Test public void testCompression_Sequence() throws Exception {
        final byte[] sourceBytes = generateSequenceInput(INPUT_SIZE);

        final ByteArrayOutputStream compressed = new ByteArrayOutputStream();

        final PigzOutputStream out = new PigzOutputStream(compressed,
                PigzOutputStream.DEFAULT_BUFSZ,
                PigzDeflaterOutputStream.DEFAULT_BLOCK_SIZE);

        out.write(sourceBytes);
        out.close();

        final byte[] inflatedBytes = inflate(compressed).toByteArray();
        assertTrue(Arrays.equals(sourceBytes, inflatedBytes));
    }

    @Test public void testFinishAlwaysCalled() throws Exception {
        final byte[] sourceBytes = generateSequenceInput(INPUT_SIZE);

        final ByteArrayOutputStream compressed = new ByteArrayOutputStream();

        final PigzOutputStream out = new PigzOutputStream(compressed);
        out.write(sourceBytes);
        out.close();

        final byte[] inflatedBytes = inflate(compressed).toByteArray();
        assertTrue(Arrays.equals(sourceBytes, inflatedBytes));
    }

    @Test public void testCompression_RandomJre() throws Exception {
        final byte[] sourceBytes = generateRandomBytes(INPUT_SIZE);

        final ByteArrayOutputStream compressed = new ByteArrayOutputStream();

        final GZIPOutputStream out = new GZIPOutputStream(compressed,PigzOutputStream.DEFAULT_BUFSZ);

        out.write(sourceBytes);
        out.finish();
        out.flush();
        out.close();

        final byte[] inflatedBytes = inflate(compressed).toByteArray();
        assertTrue(Arrays.equals(sourceBytes, inflatedBytes));
    }

    @Test public void testCompression_SequenceJre() throws Exception {
        final byte[] sourceBytes = generateSequenceInput(INPUT_SIZE);

        final ByteArrayOutputStream compressed = new ByteArrayOutputStream();

        final GZIPOutputStream out = new GZIPOutputStream(compressed,PigzOutputStream.DEFAULT_BUFSZ);

        out.write(sourceBytes);
        out.finish();
        out.flush();
        out.close();

        final byte[] inflatedBytes = inflate(compressed).toByteArray();
        assertTrue(Arrays.equals(sourceBytes, inflatedBytes));
    }

}

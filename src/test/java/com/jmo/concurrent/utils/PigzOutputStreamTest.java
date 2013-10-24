package com.jmo.concurrent.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertTrue;

public class PigzOutputStreamTest extends PigzTest {

    private static final int INPUT_SIZE = 2 * ONE_MB;
    private static final String TEST_SANDBOX = "test_sandbox";
    private static final String TEST_FILE1 = TEST_SANDBOX + "/test-file1.gz";

    @Before
    public void before() throws Exception {
        assertTrue("could not create directory: " + TEST_SANDBOX, new File(TEST_SANDBOX).mkdirs());
    }

    @After
    public void after() throws Exception {
        final File dir = new File(TEST_SANDBOX);
        assertTrue("test sandbox does not exist", dir.isDirectory());
        for ( final File file : dir.listFiles() ) {
            assertTrue("could not delete test file: " + file.getName(), file.delete());
        }
        assertTrue("could not delete directory: " + TEST_SANDBOX, dir.delete());
    }

    @Test public void testRepeat() throws Exception {
        testCompression_Random();
        testCompression_Random();
    }

    @Test public void testCompression_FileBased() throws Exception {
        final byte[] sourceBytes = generateRandomBytes(INPUT_SIZE);

        final FileOutputStream compressed = new FileOutputStream(TEST_FILE1);

        final PigzOutputStream out = new PigzOutputStream(compressed,
                PigzOutputStream.DEFAULT_BUFSZ,
                PigzDeflaterOutputStream.DEFAULT_BLOCK_SIZE);

        out.write(sourceBytes);
        out.close();

        final FileInputStream in = new FileInputStream(TEST_FILE1);
        final byte[] inflatedBytes = inflate(in).toByteArray();
        assertTrue(Arrays.equals(sourceBytes, inflatedBytes));
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

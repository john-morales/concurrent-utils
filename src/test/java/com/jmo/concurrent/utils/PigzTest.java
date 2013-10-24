package com.jmo.concurrent.utils;

import java.io.*;
import java.security.SecureRandom;
import java.util.zip.GZIPInputStream;

public abstract class PigzTest {

    protected static final int ONE_MB = 1 << 20;

    protected static final SecureRandom SECURE_RND = new SecureRandom();

    protected static ByteArrayOutputStream inflate(final FileInputStream pInputStream) throws IOException {
        final ByteArrayOutputStream uncompressed = new ByteArrayOutputStream(ONE_MB);
        final GZIPInputStream zin = new GZIPInputStream(pInputStream);

        int read;
        final byte[] buf = new byte[ONE_MB];
        while ( (read = zin.read(buf)) != -1 ) {
            uncompressed.write(buf, 0, read);
        }

        return uncompressed;
    }

    protected static ByteArrayOutputStream inflate(final ByteArrayOutputStream pOutCompressed) throws IOException {
        final ByteArrayOutputStream uncompressed = new ByteArrayOutputStream(ONE_MB);
        final GZIPInputStream zin = new GZIPInputStream(new ByteArrayInputStream(pOutCompressed.toByteArray()));

        int read;
        final byte[] buf = new byte[ONE_MB];
        while ( (read = zin.read(buf)) != -1 ) {
            uncompressed.write(buf, 0, read);
        }

        return uncompressed;
    }

    protected static byte[] generateSequenceInput(final int pSize) {
        final byte[] result = new byte[pSize];

        for ( int i = 0; i < pSize; i++ ) {
            result[i] = (byte)(i % 256);
        }

        return result;
    }

    protected static byte[] generateRandomBytes(final int pSize) {
        final byte[] result = new byte[pSize];

        SECURE_RND.nextBytes(result);
        return result;
    }
}

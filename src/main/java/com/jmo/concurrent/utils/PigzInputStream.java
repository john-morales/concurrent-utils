package com.jmo.concurrent.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * Decompression not parallelizeable.
 * Wrapper over GZIPInputStream for API consistency only.
 */
public class PigzInputStream extends GZIPInputStream {


    public PigzInputStream(final InputStream pIn) throws IOException {
        super(pIn);
    }

    public PigzInputStream(final InputStream pIn, int pSize) throws IOException {
        super(pIn, pSize);
    }

}

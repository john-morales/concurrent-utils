package com.jmo.io.stream;

import java.util.zip.Deflater;

/**
 * Guarantees Deflater implementation uses nowrap such that it's GZIP compatible.
 */

public class PigzDeflater extends Deflater {

    /**
     * @see Deflater#DEFAULT_COMPRESSION
     */
    public PigzDeflater() {
        this(DEFAULT_COMPRESSION);
    }

    /**
     * Create with custom deflate level
     * @param level 1 thru 9
     * @see Deflater
     */
    public PigzDeflater(final int level) {
        super(level, true);
    }

    /**
     * Strategy cannot be changed to remain GZIP compatible.
     * @param strategy - ignored
     */
    @Override
    public void setStrategy(final int strategy) {
    }
}

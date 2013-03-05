package com.pigz4j.io.stream;

import java.util.zip.Deflater;

/**
 * Guarantees Deflater implementation uses nowrap such that it's GZIP compatible.
 */

public class PigzDeflater extends Deflater {

    private int _level;

    /**
     * @see Deflater#DEFAULT_COMPRESSION
     */
    protected PigzDeflater() {
        this(DEFAULT_COMPRESSION);
    }

    /**
     * Create with custom deflate level
     * @param level 1 thru 9 or -1 for default
     * @see Deflater#setLevel(int)
     */
    protected PigzDeflater(final int level) {
        this(level, true);
    }

    private PigzDeflater(final int level, final boolean nowrap) {
        super(level, nowrap);
        if ( !nowrap ) { throw new IllegalStateException("nowrap required for GZIP compatibility"); }
        _level = level;
    }

    @Override
    public void setLevel(final int level) {
        if ( level != _level ) {
            super.setLevel(level);
            _level = level;
        }
    }

    protected int getLevel() {
        return _level;
    }

    /**
     * Strategy cannot be changed to remain GZIP compatible.
     * @param strategy - ignored
     */
    @Override
    public void setStrategy(final int strategy) {
    }
}

package com.pigz4j.io.stream;

import java.util.zip.Deflater;

/**
 * Returns re-usable thread local instance of Deflater with default compression.
 */
public class PigzDeflaterFactory implements IPigzDeflaterFactory {

    private final int _compressionLevel;

    public PigzDeflaterFactory(final int pCompression) {
        if ( pCompression < Deflater.BEST_SPEED && pCompression > Deflater.BEST_COMPRESSION ) {
            if ( pCompression != Deflater.DEFAULT_COMPRESSION ) {
                throw new IllegalArgumentException("invalid compression level: " + pCompression);
            }
        }
        _compressionLevel = pCompression;
    }

    /**
     * @return a reset/reinitialized thread-local instance of PigzDeflater
     * with the compression level set as this factory's level.
     * @see Deflater
     */
    public PigzDeflater getDeflater() {
        final PigzDeflater deflater = __deflater.get();
        deflater.setLevel(_compressionLevel);
        return deflater;
    }

    private static final ThreadLocal<PigzDeflater> __deflater = new ThreadLocal<PigzDeflater>() {
        @Override
        protected PigzDeflater initialValue() {
            return new PigzDeflater();
        }

        @Override
        public PigzDeflater get() {
            final PigzDeflater deflater = super.get();
            deflater.reset();
            return deflater;
        }
    };

    public static final PigzDeflaterFactory DEFAULT = new PigzDeflaterFactory(Deflater.DEFAULT_COMPRESSION);

    public static final PigzDeflaterFactory BEST_COMPRESSION = new PigzDeflaterFactory(Deflater.BEST_COMPRESSION);

    public static final PigzDeflaterFactory BEST_SPEED = new PigzDeflaterFactory(Deflater.BEST_SPEED);

}

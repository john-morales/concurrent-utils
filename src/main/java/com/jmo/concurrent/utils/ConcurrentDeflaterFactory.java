package com.jmo.concurrent.utils;

import java.util.zip.Deflater;

/**
 * Returns re-usable thread local instance of Deflater with default compression.
 */
public class ConcurrentDeflaterFactory implements IConcurrentDeflaterFactory {

    private final int _compressionLevel;

    public ConcurrentDeflaterFactory(final int pCompression) {
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
    public ConcurrentDeflater getDeflater() {
        final ConcurrentDeflater deflater = __deflater.get();
        deflater.setLevel(_compressionLevel);
        return deflater;
    }

    private static final ThreadLocal<ConcurrentDeflater> __deflater = new ThreadLocal<ConcurrentDeflater>() {
        @Override
        protected ConcurrentDeflater initialValue() {
            return new ConcurrentDeflater();
        }

        @Override
        public ConcurrentDeflater get() {
            final ConcurrentDeflater deflater = super.get();
            deflater.reset();
            return deflater;
        }
    };

    public static final ConcurrentDeflaterFactory DEFAULT = new ConcurrentDeflaterFactory(Deflater.DEFAULT_COMPRESSION);

    public static final ConcurrentDeflaterFactory BEST_COMPRESSION = new ConcurrentDeflaterFactory(Deflater.BEST_COMPRESSION);

    public static final ConcurrentDeflaterFactory BEST_SPEED = new ConcurrentDeflaterFactory(Deflater.BEST_SPEED);

}

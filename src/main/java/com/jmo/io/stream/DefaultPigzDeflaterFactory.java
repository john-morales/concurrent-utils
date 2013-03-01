package com.jmo.io.stream;

/**
 * Returns re-usable thread local instance of Deflater with default compression.
 */
public class DefaultPigzDeflaterFactory implements IPigzDeflaterFactory {

    /**
     * @return re-usable thread-local instance of PigzDeflater.
     */
    @Override
    public PigzDeflater getDeflater() {
        return __deflater.get();
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
}

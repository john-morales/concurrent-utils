package com.pigz4j.io.stream;

public interface IPigzDeflaterFactory {

    /**
     * Exposed as factory to allow for thread-local instances, pooling, etc.
     * @return PigzDeflater instance
     */
    public PigzDeflater getDeflater();

}

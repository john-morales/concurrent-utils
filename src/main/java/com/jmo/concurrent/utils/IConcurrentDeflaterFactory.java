package com.jmo.concurrent.utils;

public interface IConcurrentDeflaterFactory {

    /**
     * Exposed as factory to allow for thread-local instances, pooling, etc.
     * @return PigzDeflater instance
     */
    public ConcurrentDeflater getDeflater();

}

package com.jmo.concurrent.utils;

public interface IPigzDeflaterFactory {

    /**
     * Exposed as factory to allow for thread-local instances, pooling, etc.
     * @return PigzDeflater instance
     */
    public PigzDeflater getDeflater();

}

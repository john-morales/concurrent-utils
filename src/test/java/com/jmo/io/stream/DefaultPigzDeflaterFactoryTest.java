package com.jmo.io.stream;

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class DefaultPigzDeflaterFactoryTest {

    @Test public void getDeflater_SameThread() throws Exception {
        final DefaultPigzDeflaterFactory factory = new DefaultPigzDeflaterFactory();
        final PigzDeflater deflater = factory.getDeflater();

        assertSame(deflater, factory.getDeflater());
        assertSame(deflater, new DefaultPigzDeflaterFactory().getDeflater());
    }

    @Test public void getDeflater_DifferentThreads() throws Exception {
        final DefaultPigzDeflaterFactory factory = new DefaultPigzDeflaterFactory();
        final PigzDeflater deflater = factory.getDeflater();

        final Future<PigzDeflater> future = Executors.newSingleThreadExecutor().submit(new Callable<PigzDeflater>() {
            @Override
            public PigzDeflater call() throws Exception {
                return factory.getDeflater();
            }
        });

        assertNotSame(deflater, future.get());
        assertSame(deflater, new DefaultPigzDeflaterFactory().getDeflater());
    }

}

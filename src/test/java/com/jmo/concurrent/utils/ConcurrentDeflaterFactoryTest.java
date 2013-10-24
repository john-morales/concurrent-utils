package com.jmo.concurrent.utils;

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.Deflater;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class ConcurrentDeflaterFactoryTest {

    @Test public void getDeflater_SameThread_BasicInstances() throws Exception {
        final ConcurrentDeflaterFactory factory = new ConcurrentDeflaterFactory(Deflater.DEFAULT_COMPRESSION);
        final ConcurrentDeflater deflater = factory.getDeflater();

        assertSame(deflater, factory.getDeflater());
        assertSame(deflater, new ConcurrentDeflaterFactory(Deflater.DEFAULT_COMPRESSION).getDeflater());
        assertSame(deflater, ConcurrentDeflaterFactory.DEFAULT.getDeflater());

        assertSame(deflater.getLevel(), ConcurrentDeflaterFactory.DEFAULT.getDeflater().getLevel());
        assertSame(Deflater.DEFAULT_COMPRESSION, deflater.getLevel());
    }

    @Test public void getDeflater_SameThread_LevelInstances() throws Exception {
        final ConcurrentDeflater defaultDeflater = ConcurrentDeflaterFactory.DEFAULT.getDeflater();
        final ConcurrentDeflater bestCompressionDeflater = ConcurrentDeflaterFactory.BEST_COMPRESSION.getDeflater();
        final ConcurrentDeflater bestSpeedDeflater = ConcurrentDeflaterFactory.BEST_SPEED.getDeflater();

        assertSame(defaultDeflater, bestCompressionDeflater);
        assertSame(defaultDeflater, bestSpeedDeflater);
        assertSame(bestCompressionDeflater, bestSpeedDeflater);

        assertSame(defaultDeflater, new ConcurrentDeflaterFactory(Deflater.DEFAULT_COMPRESSION).getDeflater());
        assertSame(bestCompressionDeflater, new ConcurrentDeflaterFactory(Deflater.BEST_COMPRESSION).getDeflater());
        assertSame(bestSpeedDeflater, new ConcurrentDeflaterFactory(Deflater.BEST_SPEED).getDeflater());

        assertSame(defaultDeflater, ConcurrentDeflaterFactory.DEFAULT.getDeflater());
        assertSame(bestCompressionDeflater, ConcurrentDeflaterFactory.BEST_COMPRESSION.getDeflater());
        assertSame(bestSpeedDeflater, ConcurrentDeflaterFactory.BEST_SPEED.getDeflater());
    }

    @Test public void getDeflater_SameThread_CompressionLevels() throws Exception {
        assertEquals(Deflater.DEFAULT_COMPRESSION, ConcurrentDeflaterFactory.DEFAULT.getDeflater().getLevel());
        assertEquals(Deflater.BEST_COMPRESSION, ConcurrentDeflaterFactory.BEST_COMPRESSION.getDeflater().getLevel());
        assertEquals(Deflater.BEST_SPEED, ConcurrentDeflaterFactory.BEST_SPEED.getDeflater().getLevel());
    }

    @Test public void getDeflater_DifferentThreads() throws Exception {
        final ConcurrentDeflaterFactory factory = new ConcurrentDeflaterFactory(Deflater.DEFAULT_COMPRESSION);
        final ConcurrentDeflater deflater = factory.getDeflater();

        final Future<ConcurrentDeflater> future = Executors.newSingleThreadExecutor().submit(new Callable<ConcurrentDeflater>() {
            public ConcurrentDeflater call() throws Exception {
                return factory.getDeflater();
            }
        });

        final ConcurrentDeflater otherDeflater = future.get();
        assertNotSame(deflater, otherDeflater);
        assertSame(deflater, new ConcurrentDeflaterFactory(Deflater.DEFAULT_COMPRESSION).getDeflater());
        assertSame(Deflater.DEFAULT_COMPRESSION, ConcurrentDeflaterFactory.DEFAULT.getDeflater().getLevel());
    }

}

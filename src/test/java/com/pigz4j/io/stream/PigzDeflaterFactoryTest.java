package com.pigz4j.io.stream;

import com.pigz4j.io.stream.PigzDeflater;
import com.pigz4j.io.stream.PigzDeflaterFactory;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.Deflater;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class PigzDeflaterFactoryTest {

    @Test public void getDeflater_SameThread_BasicInstances() throws Exception {
        final PigzDeflaterFactory factory = new PigzDeflaterFactory(Deflater.DEFAULT_COMPRESSION);
        final PigzDeflater deflater = factory.getDeflater();

        assertSame(deflater, factory.getDeflater());
        assertSame(deflater, new PigzDeflaterFactory(Deflater.DEFAULT_COMPRESSION).getDeflater());
        assertSame(deflater, PigzDeflaterFactory.DEFAULT.getDeflater());

        assertSame(deflater.getLevel(), PigzDeflaterFactory.DEFAULT.getDeflater().getLevel());
        assertSame(Deflater.DEFAULT_COMPRESSION, deflater.getLevel());
    }

    @Test public void getDeflater_SameThread_LevelInstances() throws Exception {
        final PigzDeflater defaultDeflater = PigzDeflaterFactory.DEFAULT.getDeflater();
        final PigzDeflater bestCompressionDeflater = PigzDeflaterFactory.BEST_COMPRESSION.getDeflater();
        final PigzDeflater bestSpeedDeflater = PigzDeflaterFactory.BEST_SPEED.getDeflater();

        assertSame(defaultDeflater, bestCompressionDeflater);
        assertSame(defaultDeflater, bestSpeedDeflater);
        assertSame(bestCompressionDeflater, bestSpeedDeflater);

        assertSame(defaultDeflater, new PigzDeflaterFactory(Deflater.DEFAULT_COMPRESSION).getDeflater());
        assertSame(bestCompressionDeflater, new PigzDeflaterFactory(Deflater.BEST_COMPRESSION).getDeflater());
        assertSame(bestSpeedDeflater, new PigzDeflaterFactory(Deflater.BEST_SPEED).getDeflater());

        assertSame(defaultDeflater, PigzDeflaterFactory.DEFAULT.getDeflater());
        assertSame(bestCompressionDeflater, PigzDeflaterFactory.BEST_COMPRESSION.getDeflater());
        assertSame(bestSpeedDeflater, PigzDeflaterFactory.BEST_SPEED.getDeflater());
    }

    @Test public void getDeflater_SameThread_CompressionLevels() throws Exception {
        assertEquals(Deflater.DEFAULT_COMPRESSION, PigzDeflaterFactory.DEFAULT.getDeflater().getLevel());
        assertEquals(Deflater.BEST_COMPRESSION, PigzDeflaterFactory.BEST_COMPRESSION.getDeflater().getLevel());
        assertEquals(Deflater.BEST_SPEED, PigzDeflaterFactory.BEST_SPEED.getDeflater().getLevel());
    }

    @Test public void getDeflater_DifferentThreads() throws Exception {
        final PigzDeflaterFactory factory = new PigzDeflaterFactory(Deflater.DEFAULT_COMPRESSION);
        final PigzDeflater deflater = factory.getDeflater();

        final Future<PigzDeflater> future = Executors.newSingleThreadExecutor().submit(new Callable<PigzDeflater>() {
            @Override
            public PigzDeflater call() throws Exception {
                return factory.getDeflater();
            }
        });

        final PigzDeflater otherDeflater = future.get();
        assertNotSame(deflater, otherDeflater);
        assertSame(deflater, new PigzDeflaterFactory(Deflater.DEFAULT_COMPRESSION).getDeflater());
        assertSame(Deflater.DEFAULT_COMPRESSION, PigzDeflaterFactory.DEFAULT.getDeflater().getLevel());
    }

}

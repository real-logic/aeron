/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DriverNameResolverTest
{
    private final String baseDir = CommonContext.getAeronDirectoryName();

    private final ArrayList<MediaDriver> drivers = new ArrayList<>();

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(drivers);

        for (final MediaDriver driver : drivers)
        {
            driver.context().deleteDirectory();
        }
    }

    @Test
    public void shouldInitializeWithDefaultsAndHaveResolverCounters()
    {
        drivers.add(MediaDriver.launch(setDefaults(new MediaDriver.Context())));

        assertEquals(neighbors(drivers.get(0)), 0L);
    }

    @Test
    @Timeout(10)
    public void shouldSeeNeighbor()
    {
        drivers.add(MediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050")));

        drivers.add(MediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050")));

        awaitNeighbors(drivers.get(0), 1);
        awaitNeighbors(drivers.get(1), 1);
    }

    private static MediaDriver.Context setDefaults(final MediaDriver.Context context)
    {
        context
            .errorHandler(Throwable::printStackTrace)
            .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true);

        return context;
    }

    private static long neighbors(final MediaDriver driver)
    {
        final CountersReader countersReader = driver.context().countersManager();

        return countersReader.getCounterValue(SystemCounterDescriptor.RESOLVER_NEIGHBORS.id());
    }

    private static long cacheEntries(final MediaDriver driver)
    {
        final CountersReader countersReader = driver.context().countersManager();

        return countersReader.getCounterValue(SystemCounterDescriptor.RESOLVER_CACHE_ENTRIES.id());
    }

    private static void awaitNeighbors(final MediaDriver driver, final long expectedNeighbors)
    {
        while (neighbors(driver) != expectedNeighbors)
        {
            Tests.sleep(100);
            Tests.checkInterruptStatus();
        }
    }
}

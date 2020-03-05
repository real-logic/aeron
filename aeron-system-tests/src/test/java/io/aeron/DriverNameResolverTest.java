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
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.SlowTest;
import io.aeron.test.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.function.Supplier;

import static io.aeron.Aeron.NULL_VALUE;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class DriverNameResolverTest
{
    private static final SleepingMillisIdleStrategy SLEEP_50_MS = new SleepingMillisIdleStrategy(50);
    private final String baseDir = CommonContext.getAeronDirectoryName();
    private final ArrayList<TestMediaDriver> drivers = new ArrayList<>();

    @BeforeEach
    public void before()
    {
        TestMediaDriver.notSupportedOnCMediaDriverYet("Name Resolver");
    }

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(drivers);

        for (final TestMediaDriver driver : drivers)
        {
            driver.context().deleteDirectory();
        }
    }

    @Test
    public void shouldInitializeWithDefaultsAndHaveResolverCounters()
    {
        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context()
            .resolverInterface("0.0.0.0:0"))));

        final int neighborsCounterId = neighborsCounterId(drivers.get(0));
        assertNotEquals(neighborsCounterId, NULL_VALUE);
    }

    @Test
    @Timeout(10)
    public void shouldSeeNeighbor()
    {
        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050")));

        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050")));

        final int aNeighborsCounterId = neighborsCounterId(drivers.get(0));
        final int bNeighborsCounterId = neighborsCounterId(drivers.get(1));

        awaitCounterValue(drivers.get(0), aNeighborsCounterId, 1);
        awaitCounterValue(drivers.get(1), bNeighborsCounterId, 1);
    }

    @Test
    @Timeout(10)
    public void shouldSeeNeighborsViaGossip()
    {
        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050")));

        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050")));

        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-C")
            .resolverName("C")
            .resolverInterface("0.0.0.0:8052")
            .resolverBootstrapNeighbor("localhost:8051")));

        final int aNeighborsCounterId = neighborsCounterId(drivers.get(0));
        final int bNeighborsCounterId = neighborsCounterId(drivers.get(1));
        final int cNeighborsCounterId = neighborsCounterId(drivers.get(2));

        awaitCounterValue(drivers.get(0), aNeighborsCounterId, 2);
        awaitCounterValue(drivers.get(1), bNeighborsCounterId, 2);
        awaitCounterValue(drivers.get(2), cNeighborsCounterId, 2);
    }

    @Test
    @Timeout(15)
    public void shouldSeeNeighborsViaGossipAsLateJoiningDriver()
    {
        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050")));

        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050")));

        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-C")
            .resolverName("C")
            .resolverInterface("0.0.0.0:8052")
            .resolverBootstrapNeighbor("localhost:8050")));

        final int aNeighborsCounterId = neighborsCounterId(drivers.get(0));
        final int bNeighborsCounterId = neighborsCounterId(drivers.get(1));
        final int cNeighborsCounterId = neighborsCounterId(drivers.get(2));

        awaitCounterValue(drivers.get(0), aNeighborsCounterId, 2);
        awaitCounterValue(drivers.get(1), bNeighborsCounterId, 2);
        awaitCounterValue(drivers.get(2), cNeighborsCounterId, 2);

        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-D")
            .resolverName("C")
            .resolverInterface("0.0.0.0:8053")
            .resolverBootstrapNeighbor("localhost:8050")));

        final int dNeighborsCounterId = neighborsCounterId(drivers.get(3));

        awaitCounterValue(drivers.get(3), dNeighborsCounterId, 3);
        awaitCounterValue(drivers.get(0), aNeighborsCounterId, 3);
        awaitCounterValue(drivers.get(1), bNeighborsCounterId, 3);
        awaitCounterValue(drivers.get(2), cNeighborsCounterId, 3);
    }

    @Test
    @Timeout(10)
    public void shouldResolveDriverNameAndAllowConnection()
    {
        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050")));

        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050")));

        final int aNeighborsCounterId = neighborsCounterId(drivers.get(0));
        final int bNeighborsCounterId = neighborsCounterId(drivers.get(1));

        awaitCounterValue(drivers.get(0), aNeighborsCounterId, 1);
        awaitCounterValue(drivers.get(1), bNeighborsCounterId, 1);

        final int aCacheEntriesCounterId = cacheEntriesCounterId(drivers.get(0));

        awaitCounterValue(drivers.get(0), aCacheEntriesCounterId, 1);

        try (
            Aeron clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(baseDir + "-A"));
            Aeron clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(baseDir + "-B"));
            Subscription subscription = clientB.addSubscription("aeron:udp?endpoint=localhost:24325", 1);
            Publication publication = clientA.addPublication("aeron:udp?endpoint=B:24325", 1))
        {
            while (!publication.isConnected() || !subscription.isConnected())
            {
                Tests.sleep(50);
            }
        }
    }

    @SlowTest
    @Test
    @Timeout(20)
    public void shouldTimeoutAllNeighborsAndCacheEntries()
    {
        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050")));

        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050")));

        final int aNeighborsCounterId = neighborsCounterId(drivers.get(0));
        final int bNeighborsCounterId = neighborsCounterId(drivers.get(1));

        awaitCounterValue(drivers.get(0), aNeighborsCounterId, 1);
        awaitCounterValue(drivers.get(1), bNeighborsCounterId, 1);

        final int aCacheEntriesCounterId = cacheEntriesCounterId(drivers.get(0));

        awaitCounterValue(drivers.get(0), aCacheEntriesCounterId, 1);

        drivers.get(1).close();
        drivers.get(1).context().deleteDirectory();
        drivers.remove(1);

        awaitCounterValue(drivers.get(0), aNeighborsCounterId, 0);
        awaitCounterValue(drivers.get(0), aCacheEntriesCounterId, 0);
    }

    @SlowTest
    @Test
    @Timeout(30)
    public void shouldTimeoutNeighborsAndCacheEntriesThatAreSeenViaGossip()
    {
        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050")));

        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050")));

        drivers.add(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-C")
            .resolverName("C")
            .resolverInterface("0.0.0.0:8052")
            .resolverBootstrapNeighbor("localhost:8050")));

        final int aNeighborsCounterId = neighborsCounterId(drivers.get(0));
        final int bNeighborsCounterId = neighborsCounterId(drivers.get(1));
        final int cNeighborsCounterId = neighborsCounterId(drivers.get(2));

        awaitCounterValue(drivers.get(0), aNeighborsCounterId, 2);
        awaitCounterValue(drivers.get(1), bNeighborsCounterId, 2);
        awaitCounterValue(drivers.get(2), cNeighborsCounterId, 2);

        final int aCacheEntriesCounterId = cacheEntriesCounterId(drivers.get(0));
        final int bCacheEntriesCounterId = cacheEntriesCounterId(drivers.get(1));
        awaitCounterValue(drivers.get(0), aCacheEntriesCounterId, 2);
        awaitCounterValue(drivers.get(1), bCacheEntriesCounterId, 2);

        drivers.get(1).close();
        drivers.get(1).context().deleteDirectory();
        drivers.remove(1);

        awaitCounterValue(drivers.get(0), aNeighborsCounterId, 1);
        awaitCounterValue(drivers.get(0), aCacheEntriesCounterId, 1);
        awaitCounterValue(drivers.get(1), bNeighborsCounterId, 1);
        awaitCounterValue(drivers.get(1), bCacheEntriesCounterId, 1);
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

    private static int neighborsCounterId(final TestMediaDriver driver)
    {
        final CountersReader countersReader = driver.context().countersManager();
        final MutableInteger id = new MutableInteger(NULL_VALUE);

        countersReader.forEach(
            (counterId, typeId, keyBuffer, label) ->
            {
                if (label.startsWith("Resolver neighbors"))
                {
                    id.value = counterId;
                }
            });

        return id.value;
    }

    private static int cacheEntriesCounterId(final TestMediaDriver driver)
    {
        final CountersReader countersReader = driver.context().countersManager();
        final MutableInteger id = new MutableInteger(NULL_VALUE);

        countersReader.forEach(
            (counterId, typeId, keyBuffer, label) ->
            {
                if (label.startsWith("Resolver cache entries"))
                {
                    id.value = counterId;
                }
            });

        return id.value;
    }

    private static void awaitCounterValue(
        final TestMediaDriver mediaDriver,
        final int counterId,
        final long expectedValue)
    {
        final CountersReader countersReader = mediaDriver.context().countersManager();
        final Supplier<String> messageSupplier =
            () -> "Counter value: " + countersReader.getCounterValue(counterId) + ", expected: " + expectedValue;

        while (countersReader.getCounterValue(counterId) != expectedValue)
        {
            Tests.wait(SLEEP_50_MS, messageSupplier);
        }
    }
}

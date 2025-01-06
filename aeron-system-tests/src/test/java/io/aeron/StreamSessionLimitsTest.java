/*
 * Copyright 2014-2025 Real Logic Limited.
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
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(InterruptingTestCallback.class)
public class StreamSessionLimitsTest
{
    private final MediaDriver.Context context = new MediaDriver.Context();
    private TestMediaDriver driver;

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private void launch()
    {
        context
            .dirDeleteOnStart(true)
            .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100));

        driver = TestMediaDriver.launch(context, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());
        watcher.ignoreErrorsMatching(s -> s.contains("session limit"));
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(driver);
    }

    @Test
    @SlowTest
    void shouldNotConnectPublicationWhenImageCountWouldExceedLimit()
    {
        context.streamSessionLimit(2);
        launch();

        final String channel = "aeron:udp?endpoint=localhost:10000";
        final int streamId = 10000;

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()));
            Subscription sub = aeron.addSubscription(channel, streamId);
            Publication pub1 = aeron.addExclusivePublication(channel, streamId);
            Publication pub2 = aeron.addExclusivePublication(channel, streamId))
        {
            Tests.awaitConnected(sub);
            Tests.awaitConnected(pub1);
            Tests.awaitConnected(pub2);

            final Publication shouldNotConnect = aeron.addExclusivePublication(channel, streamId);

            final long initialErrorCount = errorCount(aeron);
            final long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (0 < deadlineNs - System.nanoTime())
            {
                assertFalse(shouldNotConnect.isConnected(), "should not have connected");
            }
            final long updatedErrorCount = errorCount(aeron);
            assertThat(updatedErrorCount, greaterThan(initialErrorCount));
        }
    }

    @Test
    @SlowTest
    @InterruptAfter(5)
    void shouldAllowConnectPublicationAfterImageCountExceedsLimitButPreviousPublicationHasClosed()
    {
        context.streamSessionLimit(2);
        launch();

        final String channel = "aeron:udp?endpoint=localhost:10000";
        final int streamId = 10000;

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()));
            Subscription sub = aeron.addSubscription(channel, streamId);
            Publication pub1 = aeron.addExclusivePublication(channel, streamId))
        {
            Tests.awaitConnected(sub);
            Tests.awaitConnected(pub1);

            try (Publication pub2 = aeron.addExclusivePublication(channel, streamId))
            {
                Tests.awaitConnected(pub2);
                final long initialErrorCount = errorCount(aeron);

                try (Publication shouldNotConnect = aeron.addExclusivePublication(channel, streamId))
                {
                    while (initialErrorCount == errorCount(aeron))
                    {
                        assertFalse(shouldNotConnect.isConnected());
                        Tests.yield();
                    }
                }
            }

            while (2 <= sub.imageCount())
            {
                Tests.yield();
            }

            final Publication shouldNowConnect = aeron.addExclusivePublication(channel, streamId);

            while (!shouldNowConnect.isConnected())
            {
                Tests.yieldingIdle("Never connected");
            }
        }
    }

    @Test
    @SlowTest
    void shouldSessionLimitsShouldBeScopedToChannelAndStream()
    {
        context.streamSessionLimit(1);
        launch();

        final String channel1 = "aeron:udp?endpoint=localhost:10000";
        final int streamId1 = 10000;
        final String channel2 = "aeron:udp?endpoint=localhost:10001";
        final int streamId2 = 10001;

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()));
            Subscription sub1 = aeron.addSubscription(channel1, streamId1);
            Subscription sub2 = aeron.addSubscription(channel1, streamId2);
            Subscription sub3 = aeron.addSubscription(channel2, streamId1);
            Subscription sub4 = aeron.addSubscription(channel2, streamId2);
            Publication pub1 = aeron.addExclusivePublication(channel1, streamId1);
            Publication pub2 = aeron.addExclusivePublication(channel2, streamId1);
            Publication pub3 = aeron.addExclusivePublication(channel1, streamId2);
            Publication pub4 = aeron.addExclusivePublication(channel2, streamId2))
        {
            Tests.awaitConnected(sub1);
            Tests.awaitConnected(sub2);
            Tests.awaitConnected(sub3);
            Tests.awaitConnected(sub4);
            Tests.awaitConnected(pub1);
            Tests.awaitConnected(pub2);
            Tests.awaitConnected(pub3);
            Tests.awaitConnected(pub4);
        }
    }

    private static long errorCount(final Aeron aeron)
    {
        return aeron.countersReader().getCounterValue(SystemCounterDescriptor.ERRORS.id());
    }
}

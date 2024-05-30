/*
 * Copyright 2014-2024 Real Logic Limited.
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
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class SubscriberEndOfStreamTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private final DirectBuffer message = new UnsafeBuffer("this is a test message".getBytes(US_ASCII));

    private final MediaDriver.Context context = new MediaDriver.Context()
        .dirDeleteOnStart(true)
        .threadingMode(ThreadingMode.SHARED);
    private TestMediaDriver driver;

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(driver);
    }

    private TestMediaDriver launch()
    {
        driver = TestMediaDriver.launch(context, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());
        return driver;
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=localhost:10000|linger=0s",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/1",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max"
    })
    @InterruptAfter(20)
    @SlowTest
    void shouldDisconnectPublicationWithEosIfSubscriptionIsClosed(final String channel)
    {
        TestMediaDriver.notSupportedOnCMediaDriver("Not yet implemented");

        final TestMediaDriver driver = launch();
        final int streamId = 10000;

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName());

        final AtomicBoolean imageUnavailable = new AtomicBoolean(false);

        try (Aeron aeron = Aeron.connect(ctx);
            Publication pub = aeron.addPublication(channel, streamId))
        {
            final Subscription sub = aeron.addSubscription(
                channel, streamId, image -> {}, image -> imageUnavailable.set(true));

            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            while (pub.offer(message) < 0)
            {
                Tests.yield();
            }

            while (0 == sub.poll((buffer, offset, length, header) -> {}, 1))
            {
                Tests.yield();
            }

            final Image image = sub.imageAtIndex(0);
            assertEquals(pub.position(), image.position());

            CloseHelper.close(sub);

            final long t0 = System.nanoTime();
            while (pub.isConnected())
            {
                Tests.yield();
            }
            final long t1 = System.nanoTime();
            assertThat(t1 - t0, lessThan(driver.context().publicationConnectionTimeoutNs()));

            while (!imageUnavailable.get())
            {
                Tests.yield();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?control-mode=dynamic|control=localhost:10000|fc=min",
        "aeron:udp?control-mode=dynamic|control=localhost:10000|fc=max",
    })
    @InterruptAfter(20)
    @SlowTest
    void shouldDisconnectPublicationWithEosIfSubscriptionIsClosedMdc(final String publicationChannel)
    {
        TestMediaDriver.notSupportedOnCMediaDriver("Not yet implemented");

        final TestMediaDriver driver = launch();
        final int streamId = 10000;
        final String subscriptionChannel = "aeron:udp?control=localhost:10000";

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName());

        final AtomicBoolean imageUnavailable = new AtomicBoolean(false);

        try (Aeron aeron = Aeron.connect(ctx);
            Publication pub = aeron.addPublication(publicationChannel, streamId))
        {
            final Subscription sub = aeron.addSubscription(
                subscriptionChannel, streamId, image -> {}, image -> imageUnavailable.set(true));

            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            while (pub.offer(message) < 0)
            {
                Tests.yield();
            }

            while (0 == sub.poll((buffer, offset, length, header) -> {}, 1))
            {
                Tests.yield();
            }

            final Image image = sub.imageAtIndex(0);
            assertEquals(pub.position(), image.position());

            CloseHelper.close(sub);

            final long t0 = System.nanoTime();
            while (pub.isConnected())
            {
                Tests.yield();
            }
            final long t1 = System.nanoTime();
            assertThat(t1 - t0, lessThan(driver.context().publicationConnectionTimeoutNs()));

            while (!imageUnavailable.get())
            {
                Tests.yield();
            }
        }
    }
}

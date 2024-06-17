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
import io.aeron.exceptions.AeronException;
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
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.aeron.driver.status.SystemCounterDescriptor.ERRORS;
import static io.aeron.driver.status.SystemCounterDescriptor.ERROR_FRAMES_RECEIVED;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class ImageInvalidationTest
{
    public static final long A_VALUE_THAT_SHOWS_WE_ARENT_SPAMMING_ERROR_MESSAGES = 1000L;
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private final String channel = "aeron:udp?endpoint=localhost:10000";
    private final int streamId = 10000;
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
        return driver;
    }

    @Test
    @InterruptAfter(10)
    @SlowTest
    void shouldInvalidateSubscriptionsImage() throws IOException
    {
        context.imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(3));

        final TestMediaDriver driver = launch();

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName());

        final AtomicBoolean imageUnavailable = new AtomicBoolean(false);

        try (Aeron aeron = Aeron.connect(ctx);
            Publication pub = aeron.addPublication(channel, streamId);
            Subscription sub = aeron.addSubscription(channel, streamId, null, image -> imageUnavailable.set(true)))
        {
            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            final CountersReader countersReader = aeron.countersReader();
            final long initialErrorFramesReceived = countersReader
                .getCounterValue(ERROR_FRAMES_RECEIVED.id());
            final long initialErrors = countersReader
                .getCounterValue(ERRORS.id());

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

            final String reason = "Needs to be closed";
            image.invalidate(reason);

            final long t0 = System.nanoTime();
            while (pub.isConnected())
            {
                Tests.yield();
            }
            final long t1 = System.nanoTime();
            final long value = driver.context().publicationConnectionTimeoutNs();
            assertThat(t1 - t0, lessThan(value));

            while (initialErrorFramesReceived == countersReader
                .getCounterValue(ERROR_FRAMES_RECEIVED.id()))
            {
                Tests.yield();
            }

            while (initialErrors == countersReader
                .getCounterValue(ERRORS.id()))
            {
                Tests.yield();
            }

            while (!imageUnavailable.get())
            {
                Tests.yield();
            }

            assertThat(
                countersReader.getCounterValue(ERROR_FRAMES_RECEIVED.id()) - initialErrorFramesReceived,
                lessThan(A_VALUE_THAT_SHOWS_WE_ARENT_SPAMMING_ERROR_MESSAGES));

            assertEquals(1, countersReader.getCounterValue(ERRORS.id()) - initialErrors);

            SystemTests.waitForErrorToOccur(driver.aeronDirectoryName(), containsString(reason), Tests.SLEEP_1_MS);
        }
    }

    @Test
    @InterruptAfter(10)
    @SlowTest
    void shouldInvalidateSubscriptionsImageManualMdc()
    {
        context.imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(3));

        final TestMediaDriver driver = launch();

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName());

        final AtomicInteger imageAvailable = new AtomicInteger(0);
        final AtomicInteger imageUnavailable = new AtomicInteger(0);
        final String mdc = "aeron:udp?control-mode=manual";
        final String channel = "aeron:udp?endpoint=localhost:10000";

        try (Aeron aeron = Aeron.connect(ctx);
            Publication pub = aeron.addPublication(mdc, streamId);
            Subscription sub = aeron.addSubscription(
                channel,
                streamId,
                (image) -> imageAvailable.incrementAndGet(),
                (image) -> imageUnavailable.incrementAndGet()))
        {
            pub.addDestination(channel);

            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            final long initialErrorMessagesReceived = aeron.countersReader()
                .getCounterValue(ERROR_FRAMES_RECEIVED.id());

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

            final int initialAvailable = imageAvailable.get();
            final String reason = "Needs to be closed";
            image.invalidate(reason);

            final long t0 = System.nanoTime();
            while (pub.isConnected())
            {
                Tests.yield();
            }
            final long t1 = System.nanoTime();
            final long value = driver.context().publicationConnectionTimeoutNs();
            assertThat(t1 - t0, lessThan(value));

            while (initialErrorMessagesReceived == aeron.countersReader()
                .getCounterValue(ERROR_FRAMES_RECEIVED.id()))
            {
                Tests.yield();
            }

            while (0 == imageUnavailable.get())
            {
                Tests.yield();
            }

            assertThat(
                aeron.countersReader().getCounterValue(ERROR_FRAMES_RECEIVED.id()),
                lessThan(A_VALUE_THAT_SHOWS_WE_ARENT_SPAMMING_ERROR_MESSAGES));

            while (initialAvailable != imageAvailable.get())
            {
                Tests.yield();
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldRejectInvalidationReasonThatIsTooLong()
    {
        context.imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(3));
        final byte[] bytes = new byte[1024];
        Arrays.fill(bytes, (byte)'x');
        final String tooLongReason = new String(bytes, US_ASCII);

        final TestMediaDriver driver = launch();

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName());

        try (Aeron aeron = Aeron.connect(ctx);
            Publication pub = aeron.addPublication(channel, streamId);
            Subscription sub = aeron.addSubscription(channel, streamId))
        {
            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            assertThrows(AeronException.class, () -> sub.imageAtIndex(0).invalidate(tooLongReason));
        }
    }
}

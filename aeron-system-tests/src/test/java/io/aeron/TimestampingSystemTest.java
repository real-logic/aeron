/*
 * Copyright 2014-2021 Real Logic Limited.
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
import io.aeron.exceptions.RegistrationException;
import io.aeron.log.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import io.aeron.test.driver.DistinctErrorLogTestWatcher;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(InterruptingTestCallback.class)
public class TimestampingSystemTest
{
    public static final long SENTINEL_VALUE = -1L;
    public static final String CHANNEL_WITH_RX_TIMESTAMP = "aeron:udp?endpoint=localhost:0|rx-ts-offset=reserved";
    public static final String CHANNEL_WITH_SND_RCV_TIMESTAMPS =
        "aeron:udp?endpoint=localhost:0|rcv-ts-offset=0|snd-ts-offset=8";

    @RegisterExtension
    public final MediaDriverTestWatcher watcher = new MediaDriverTestWatcher();

    @RegisterExtension
    public final DistinctErrorLogTestWatcher logWatcher = new DistinctErrorLogTestWatcher();

    @Test
    void shouldErrorOnRxTimestampsInJavaDriver()
    {
        assumeTrue(TestMediaDriver.shouldRunJavaMediaDriver());

        try (TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context(), watcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            assertThrows(
                RegistrationException.class,
                () -> aeron.addSubscription(CHANNEL_WITH_RX_TIMESTAMP, 1000));
        }
    }

    @Test
    void shouldSupportRxTimestampsInCDriver()
    {
        assumeTrue(TestMediaDriver.shouldRunCMediaDriver());

        final DirectBuffer buffer = new UnsafeBuffer(new byte[64]);

        try (TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context(), watcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final Subscription sub = aeron.addSubscription(CHANNEL_WITH_RX_TIMESTAMP, 1000);

            while (null == sub.resolvedEndpoint())
            {
                Tests.yieldingIdle("Failed to resolve endpoint");
            }

            final String uri = new ChannelUriStringBuilder()
                .media("udp")
                .endpoint(sub.resolvedEndpoint())
                .build();

            final Publication pub = aeron.addPublication(uri, 1000);

            Tests.awaitConnected(pub);

            while (0 > pub.offer(buffer, 0, buffer.capacity(), (termBuffer, termOffset, frameLength) -> SENTINEL_VALUE))
            {
                Tests.yieldingIdle("Failed to offer message");
            }

            while (1 > sub.poll(
                (buffer1, offset, length, header) -> assertNotEquals(SENTINEL_VALUE, header.reservedValue()), 1))
            {
                Tests.yieldingIdle("Failed to receive message");
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldSupportSendReceiveTimestamps()
    {
        final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[64]);

        final MediaDriver.Context context = new MediaDriver.Context()
            .dirDeleteOnStart(true);
        final String aeronDirectoryName = context.aeronDirectoryName();

        try (TestMediaDriver driver = TestMediaDriver.launch(context, watcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final Subscription sub = aeron.addSubscription(CHANNEL_WITH_SND_RCV_TIMESTAMPS, 1000);

            while (null == sub.resolvedEndpoint())
            {
                Tests.yieldingIdle("Failed to resolve endpoint");
            }

            final String uri = new ChannelUriStringBuilder(CHANNEL_WITH_SND_RCV_TIMESTAMPS)
                .endpoint(requireNonNull(sub.resolvedEndpoint()))
                .build();

            final Publication pub = aeron.addPublication(uri, 1000);

            Tests.awaitConnected(pub);

            buffer.putLong(0, SENTINEL_VALUE);
            buffer.putLong(8, SENTINEL_VALUE);

            while (0 > pub.offer(buffer, 0, buffer.capacity()))
            {
                Tests.yieldingIdle("Failed to offer message");
            }

            final MutableLong receiveTimestamp = new MutableLong(SENTINEL_VALUE);
            final MutableLong sendTimestamp = new MutableLong(SENTINEL_VALUE);
            while (1 > sub.poll(
                (buffer1, offset, length, header) ->
                {
                    receiveTimestamp.set(buffer1.getLong(offset));
                    sendTimestamp.set(buffer1.getLong(offset + 8));
                }, 1))
            {
                Tests.yieldingIdle("Failed to receive message");
            }

            assertNotEquals(SENTINEL_VALUE, receiveTimestamp.longValue());
            assertNotEquals(SENTINEL_VALUE, sendTimestamp.longValue());
        }
        finally
        {
            logWatcher.captureErrors(aeronDirectoryName);
        }
    }
}

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
import io.aeron.test.Tests;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TimestampingSystemTest
{
    public static final long SENTINEL_VALUE = -1L;
    public static final String CHANNEL_WITH_PACKET_TIMESTAMP = "aeron:udp?endpoint=localhost:0|pkt-ts-offset=reserved";

    @RegisterExtension
    public final MediaDriverTestWatcher watcher = new MediaDriverTestWatcher();

    @Test
    void shouldErrorOnPacketTimestampsInJavaDriver()
    {
        assumeTrue(TestMediaDriver.shouldRunJavaMediaDriver());

        try (TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context(), watcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            assertThrows(
                RegistrationException.class,
                () -> aeron.addSubscription(CHANNEL_WITH_PACKET_TIMESTAMP, 1000));
        }
    }

    @Test
    void shouldSupportPacketTimestampsInJavaDriver()
    {
        assumeTrue(TestMediaDriver.shouldRunCMediaDriver());

        final DirectBuffer buffer = new UnsafeBuffer(new byte[64]);

        try (TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context(), watcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final Subscription sub = aeron.addSubscription(CHANNEL_WITH_PACKET_TIMESTAMP, 1000);
            final String uri = new ChannelUriStringBuilder()
                .media("udp")
                .endpoint(sub.resolvedEndpoint())
                .build();

            final Publication pub = aeron.addPublication(uri, 1000);

            Tests.awaitConnected(pub);

            while (0 < pub.offer(buffer, 0, buffer.capacity(), (termBuffer, termOffset, frameLength) -> SENTINEL_VALUE))
            {
                Tests.yieldingIdle("Failed to offer message");
            }

            while (1 < sub.poll(
                (buffer1, offset, length, header) -> assertNotEquals(SENTINEL_VALUE, header.reservedValue()), 1))
            {
                Tests.yieldingIdle("Failed to receive message");
            }
        }
    }
}

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
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.status.LocalSocketAddressStatus;
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(InterruptingTestCallback.class)
@SlowTest
class SubscriptionReconnectTest
{
    private static final int STREAM_ID = 1001;

    private static List<String> channels()
    {
        return Arrays.asList(
            "aeron:udp?endpoint=localhost:3333|linger=0",
            "aeron:udp?endpoint=localhost:3333|linger=0|session-id=1");
    }

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        driver = TestMediaDriver.launch(driverCtx, testWatcher);
        testWatcher.dataCollector().add(driverCtx.aeronDirectory());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.close(driver);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldAddExclusivePublication(final String channel)
    {
        try (Aeron aeron = Aeron.connect();
            Subscription subscription = aeron.addSubscription(channel, STREAM_ID))
        {
            final BufferClaim bufferClaim = new BufferClaim();
            final MutableInteger value = new MutableInteger();
            final FragmentHandler handler = (buffer, offset, length, header) -> value.set(buffer.getInt(offset));

            for (int i = 0; i < 3; i++)
            {
                final Publication publication = aeron.addExclusivePublication(channel, STREAM_ID);

                while (!LocalSocketAddressStatus.isActive(aeron.countersReader(), publication.channelStatusId()))
                {
                    Tests.yield();
                }

                Tests.awaitConnected(subscription);

                while (true)
                {
                    if (publication.tryClaim(SIZE_OF_INT, bufferClaim) > 0)
                    {
                        bufferClaim.buffer().putInt(bufferClaim.offset(), i);
                        bufferClaim.commit();
                        break;
                    }

                    Tests.yield();
                }

                value.set(Aeron.NULL_VALUE);

                while (subscription.poll(handler, 1) <= 0)
                {
                    Tests.yield();
                }

                assertEquals(i, value.get());

                publication.close();

                while (LocalSocketAddressStatus.isActive(aeron.countersReader(), publication.channelStatusId()))
                {
                    Tests.yield();
                }
            }
        }
    }
}

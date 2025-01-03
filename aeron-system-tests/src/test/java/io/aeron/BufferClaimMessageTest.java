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
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(InterruptingTestCallback.class)
class BufferClaimMessageTest
{
    private static List<String> channels()
    {
        return Arrays.asList("aeron:udp?endpoint=localhost:24325", CommonContext.IPC_CHANNEL);
    }

    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = 200;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void setUp()
    {
        driver = TestMediaDriver.launch(new MediaDriver.Context()
                .errorHandler(Tests::onError)
                .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
                .threadingMode(ThreadingMode.SHARED),
            testWatcher);
        testWatcher.dataCollector().add(driver.context().aeronDirectory());

        aeron = Aeron.connect();
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldReceivePublishedMessageWithInterleavedAbort(final String channel)
    {
        final MutableInteger fragmentCount = new MutableInteger();
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> fragmentCount.value++;

        final BufferClaim bufferClaim = new BufferClaim();
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));

        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            Publication publication = aeron.addPublication(channel, STREAM_ID))
        {
            publishMessage(srcBuffer, publication);

            while (publication.tryClaim(MESSAGE_LENGTH, bufferClaim) < 0L)
            {
                Tests.yield();
            }

            publishMessage(srcBuffer, publication);

            bufferClaim.abort();

            final int expectedNumberOfFragments = 2;
            int numFragments = 0;
            do
            {
                final int fragments = subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
                if (0 == fragments)
                {
                    Tests.yield();
                }

                numFragments += fragments;
            }
            while (numFragments < expectedNumberOfFragments);

            assertEquals(expectedNumberOfFragments, fragmentCount.value);
        }
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldTransferReservedValue(final String channel)
    {
        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            Publication publication = aeron.addPublication(channel, STREAM_ID))
        {
            final BufferClaim bufferClaim = new BufferClaim();
            while (publication.tryClaim(MESSAGE_LENGTH, bufferClaim) < 0L)
            {
                Tests.yield();
            }

            final long reservedValue = System.currentTimeMillis();
            bufferClaim.reservedValue(reservedValue);
            bufferClaim.commit();

            final MutableBoolean done = new MutableBoolean();
            final FragmentHandler fragmentHandler =
                (buffer, offset, length, header) ->
                {
                    assertEquals(MESSAGE_LENGTH, length);
                    assertEquals(reservedValue, header.reservedValue());
                    done.value = true;
                };

            while (!done.get())
            {
                final int fragments = subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
                if (0 == fragments)
                {
                    Tests.yield();
                }
            }
        }
    }

    private static void publishMessage(final UnsafeBuffer srcBuffer, final Publication publication)
    {
        while (publication.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
        {
            Tests.yield();
        }
    }
}

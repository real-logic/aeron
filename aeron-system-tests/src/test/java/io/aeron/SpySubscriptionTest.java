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
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static io.aeron.CommonContext.SPY_PREFIX;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(InterruptingTestCallback.class)
class SpySubscriptionTest
{
    private static List<String> channels()
    {
        return asList(
            "aeron:udp?endpoint=localhost:24325",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost");
    }

    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int PAYLOAD_LENGTH = 10;

    private final MutableInteger fragmentCountSpy = new MutableInteger();
    private final FragmentHandler fragmentHandlerSpy = (buffer1, offset, length, header) -> fragmentCountSpy.value++;

    private final MutableInteger fragmentCountSub = new MutableInteger();
    private final FragmentHandler fragmentHandlerSub = (buffer1, offset, length, header) -> fragmentCountSub.value++;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void setUp()
    {
        driver = TestMediaDriver.launch(new MediaDriver.Context()
                .errorHandler(Tests::onError)
                .dirDeleteOnStart(true)
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
    void shouldReceivePublishedMessage(final String channel)
    {
        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            Subscription spy = aeron.addSubscription(SPY_PREFIX + channel, STREAM_ID);
            Publication publication = aeron.addPublication(channel, STREAM_ID))
        {
            final int expectedMessageCount = 4;
            final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[PAYLOAD_LENGTH * expectedMessageCount]);

            for (int i = 0; i < expectedMessageCount; i++)
            {
                srcBuffer.setMemory(i * PAYLOAD_LENGTH, PAYLOAD_LENGTH, (byte)(65 + i));
            }

            for (int i = 0; i < expectedMessageCount; i++)
            {
                while (publication.offer(srcBuffer, i * PAYLOAD_LENGTH, PAYLOAD_LENGTH) < 0L)
                {
                    Tests.yield();
                }
            }

            int numFragments = 0;
            int numSpyFragments = 0;
            do
            {
                Tests.yield();

                numFragments += subscription.poll(fragmentHandlerSub, FRAGMENT_COUNT_LIMIT);
                numSpyFragments += spy.poll(fragmentHandlerSpy, FRAGMENT_COUNT_LIMIT);
            }
            while (numSpyFragments < expectedMessageCount || numFragments < expectedMessageCount);

            assertEquals(expectedMessageCount, fragmentCountSpy.value);
            assertEquals(expectedMessageCount, fragmentCountSub.value);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldConnectToRecreatedChannelByTag()
    {
        final long tag1 = aeron.nextCorrelationId();
        final String channelOne = new ChannelUriStringBuilder()
            .media("udp")
            .tags(tag1, null)
            .endpoint("localhost:24325")
            .build();
        final ChannelUriStringBuilder spyChannelOneBuilder = new ChannelUriStringBuilder()
            .prefix("aeron-spy")
            .media("udp")
            .tags(tag1, null);

        try (Publication publication = aeron.addExclusivePublication(channelOne, STREAM_ID);
            Subscription spy = aeron.addSubscription(
                spyChannelOneBuilder.sessionId(publication.sessionId()).build(), STREAM_ID))
        {
            Tests.await(spy::isConnected);
            assertNotNull(spy.imageBySessionId(publication.sessionId()));
        }

        final long tag2 = aeron.nextCorrelationId();
        final String channelTwo = new ChannelUriStringBuilder()
            .media("udp")
            .tags(tag2, null)
            .endpoint("localhost:24325")
            .build();
        final ChannelUriStringBuilder spyChannelTwoBuilder = new ChannelUriStringBuilder()
            .prefix("aeron-spy")
            .media("udp")
            .tags(tag2, null);

        try (Publication publication = aeron.addExclusivePublication(channelTwo, STREAM_ID);
            Subscription spy = aeron.addSubscription(
                spyChannelTwoBuilder.sessionId(publication.sessionId()).build(), STREAM_ID))
        {
            Tests.await(spy::isConnected);
            assertNotNull(spy.imageBySessionId(publication.sessionId()));
        }
    }
}

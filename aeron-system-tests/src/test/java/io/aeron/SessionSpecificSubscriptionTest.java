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
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static io.aeron.CommonContext.IPC_MEDIA;
import static io.aeron.CommonContext.UDP_MEDIA;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

@ExtendWith(InterruptingTestCallback.class)
@InterruptAfter(10)
class SessionSpecificSubscriptionTest
{
    private static final String ENDPOINT = "localhost:24325";
    private static final int SESSION_ID_1 = 1077;
    private static final int SESSION_ID_2 = 1078;
    private static final int STREAM_ID = 1007;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = 1024 - DataHeaderFlyweight.HEADER_LENGTH;
    private static final int EXPECTED_NUMBER_OF_MESSAGES = 10;

    private final FragmentHandler mockFragmentHandler = mock(FragmentHandler.class);
    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private static Stream<ChannelUriStringBuilder> data()
    {
        return Stream.of(
            new ChannelUriStringBuilder().media(UDP_MEDIA).endpoint(ENDPOINT),
            new ChannelUriStringBuilder().media(IPC_MEDIA));
    }

    private final FragmentHandler handlerSessionIdOne =
        (buffer, offset, length, header) -> assertEquals(SESSION_ID_1, header.sessionId());
    private final FragmentHandler handlerSessionIdTwo =
        (buffer, offset, length, header) -> assertEquals(SESSION_ID_2, header.sessionId());

    private final TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Tests::onError)
        .dirDeleteOnStart(true)
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED),
        systemTestWatcher);

    private final Aeron aeron = Aeron.connect();

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
        driver.context().deleteDirectory();
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldSubscribeToSpecificSessionIdsAndWildcard(final ChannelUriStringBuilder channelBuilder)
    {
        final String channelUriWithoutSessionId = channelBuilder.build();
        final String channelUriWithSessionIdOne = channelBuilder.sessionId(SESSION_ID_1).build();
        final String channelUriWithSessionIdTwo = channelBuilder.sessionId(SESSION_ID_2).build();

        try (Subscription subscriptionOne = aeron.addSubscription(channelUriWithSessionIdOne, STREAM_ID);
            Subscription subscriptionTwo = aeron.addSubscription(channelUriWithSessionIdTwo, STREAM_ID);
            Subscription subscriptionWildcard = aeron.addSubscription(channelUriWithoutSessionId, STREAM_ID);
            Publication publicationOne = aeron.addExclusivePublication(channelUriWithSessionIdOne, STREAM_ID);
            Publication publicationTwo = aeron.addExclusivePublication(channelUriWithSessionIdTwo, STREAM_ID))
        {
            while (subscriptionOne.imageCount() != 1 ||
                subscriptionTwo.imageCount() != 1 ||
                subscriptionWildcard.imageCount() != 2)
            {
                Tests.yield();
            }

            for (int i = 0; i < EXPECTED_NUMBER_OF_MESSAGES; i++)
            {
                publishMessage(srcBuffer, publicationOne);
                publishMessage(srcBuffer, publicationTwo);
            }

            int numFragments = 0;
            do
            {
                Tests.checkInterruptStatus();
                numFragments += subscriptionOne.poll(handlerSessionIdOne, FRAGMENT_COUNT_LIMIT);
            }
            while (numFragments < EXPECTED_NUMBER_OF_MESSAGES);

            numFragments = 0;
            do
            {
                Tests.checkInterruptStatus();
                numFragments += subscriptionTwo.poll(handlerSessionIdTwo, FRAGMENT_COUNT_LIMIT);
            }
            while (numFragments < EXPECTED_NUMBER_OF_MESSAGES);

            numFragments = 0;
            do
            {
                Tests.checkInterruptStatus();
                numFragments += subscriptionWildcard.poll(mockFragmentHandler, FRAGMENT_COUNT_LIMIT);
            }
            while (numFragments < (EXPECTED_NUMBER_OF_MESSAGES * 2));
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldNotSubscribeWithoutSpecificSession(final ChannelUriStringBuilder channelBuilder)
    {
        final String channelUriWithoutSessionId = channelBuilder.build();
        final String channelUriWithSessionIdOne = channelBuilder.sessionId(SESSION_ID_1).build();
        final String channelUriWithSessionIdTwo = channelBuilder.sessionId(SESSION_ID_2).build();

        try (Subscription subscription = aeron.addSubscription(channelUriWithSessionIdOne, STREAM_ID);
            Publication publication = aeron.addExclusivePublication(channelUriWithSessionIdOne, STREAM_ID);
            Publication publicationWildcard = aeron.addExclusivePublication(channelUriWithoutSessionId, STREAM_ID);
            Publication publicationWrongSession = aeron.addExclusivePublication(channelUriWithSessionIdTwo, STREAM_ID))
        {
            Tests.awaitConnected(publication);

            assertEquals(1, subscription.imageCount());

            for (int i = 0; i < EXPECTED_NUMBER_OF_MESSAGES; i++)
            {
                publishMessage(srcBuffer, publication);
            }

            int numFragments = 0;
            do
            {
                Tests.checkInterruptStatus();
                numFragments += subscription.poll(handlerSessionIdOne, FRAGMENT_COUNT_LIMIT);
            }
            while (numFragments < EXPECTED_NUMBER_OF_MESSAGES);

            assertFalse(publicationWildcard.isConnected());
            assertFalse(publicationWrongSession.isConnected());
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldOnlySeeDataOnSpecificSessionWhenUsingTags(final ChannelUriStringBuilder channelBuilder)
    {
        final DirectBuffer liveBuffer = new UnsafeBuffer("live".getBytes(StandardCharsets.US_ASCII));
        final DirectBuffer ignoredBuffer = new UnsafeBuffer("ignored".getBytes(StandardCharsets.US_ASCII));

        final String liveChannel = channelBuilder.sessionId(SESSION_ID_1).build();
        final String generalChannel = channelBuilder.sessionId((String)null).build();

        final long channelTag = aeron.nextCorrelationId();
        final long subscriptionTag = aeron.nextCorrelationId();

        final String endpointChannel = channelBuilder.tags(channelTag, subscriptionTag).build();
        final String tagOnlyChannel = channelBuilder
            .endpoint((String)null)
            .tags(channelTag, subscriptionTag)
            .sessionId(SESSION_ID_1)
            .build();

        try (
            Publication livePub = aeron.addExclusivePublication(liveChannel, STREAM_ID);
            Publication ignoredPub = aeron.addExclusivePublication(generalChannel, STREAM_ID);
            Subscription endpointSub = aeron.addSubscription(endpointChannel, STREAM_ID);
            Subscription tagOnlySub = aeron.addSubscription(tagOnlyChannel, STREAM_ID))
        {
            Tests.awaitConnected(livePub);
            Tests.awaitConnected(endpointSub);
            Tests.awaitConnected(tagOnlySub);
            Tests.awaitConnected(ignoredPub);

            while (livePub.offer(liveBuffer) < 0)
            {
                Tests.yield();
            }

            while (ignoredPub.offer(ignoredBuffer) < 0)
            {
                Tests.yield();
            }

            final MutableBoolean isValid = new MutableBoolean(true);
            final long deadlineMs = System.currentTimeMillis() + 2_000;
            final IdleStrategy idleStrategy = new YieldingIdleStrategy();

            final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
            {
                final String s = buffer.getStringWithoutLengthAscii(offset, length);
                if ("ignored".equals(s))
                {
                    isValid.set(false);
                }
            };

            int totalFragments = 0;
            while (System.currentTimeMillis() < deadlineMs)
            {
                final int fragments = tagOnlySub.poll(fragmentHandler, 10);
                totalFragments += fragments;
                idleStrategy.idle(fragments);
            }

            assertTrue(isValid.get());
            assertEquals(1, totalFragments);
        }
    }

    @Test
    void shouldNotSeeNotificationsForSessionsThatAreNotRelevant()
    {
        final String uri1 = "aeron:udp?endpoint=localhost:20000|session-id=12345|rejoin=false";
        final String uri2 = "aeron:udp?endpoint=localhost:20000|session-id=12346|rejoin=false";

        final AtomicReference<String> error1 = new AtomicReference<>(null);
        final AtomicReference<String> error2 = new AtomicReference<>(null);
        final AvailableImageHandler handler1 = (image) ->
        {
            if (image.sessionId() != 12345)
            {
                error1.compareAndSet(null, image.toString());
            }
        };
        final AvailableImageHandler handler2 = (image) ->
        {
            if (image.sessionId() != 12346)
            {
                error2.compareAndSet(null, image.toString());
            }
        };

        try (
            Subscription sub1 = aeron.addSubscription(uri1, 10000, handler1, image -> {}))
        {
            try (
                ExclusivePublication pub1 = aeron.addExclusivePublication(uri1, 10000))
            {
                Tests.awaitConnected(sub1);
                Tests.awaitConnected(pub1);

                try (Subscription sub2 = aeron.addSubscription(uri2, 10000, handler2, image -> {}))
                {
                    Objects.requireNonNull(sub2);
                    Tests.sleep(500);
                }
            }
        }

        assertNull(error1.get());
        assertNull(error2.get());
    }

    @Test
    void shouldNotSeeNotificationsForSessionsThatAreNotRelevantViaIpc()
    {
        final String uri1 = "aeron:ipc?session-id=12345|rejoin=false";
        final String uri2 = "aeron:ipc?session-id=12346|rejoin=false";

        final AtomicReference<String> error1 = new AtomicReference<>(null);
        final AtomicReference<String> error2 = new AtomicReference<>(null);
        final AvailableImageHandler handler1 = (image) ->
        {
            if (image.sessionId() != 12345)
            {
                error1.compareAndSet(null, image.toString());
            }
        };
        final AvailableImageHandler handler2 = (image) ->
        {
            if (image.sessionId() != 12346)
            {
                error2.compareAndSet(null, image.toString());
            }
        };

        try (
            Subscription sub1 = aeron.addSubscription(uri1, 10000, handler1, image -> {}))
        {
            try (
                ExclusivePublication pub1 = aeron.addExclusivePublication(uri1, 10000))
            {
                Tests.awaitConnected(sub1);
                Tests.awaitConnected(pub1);

                try (Subscription sub2 = aeron.addSubscription(uri2, 10000, handler2, image -> {}))
                {
                    Objects.requireNonNull(sub2);
                    Tests.sleep(500);
                }
            }
        }

        assertNull(error1.get());
        assertNull(error2.get());
    }

    private static void publishMessage(final UnsafeBuffer buffer, final Publication publication)
    {
        while (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0L)
        {
            Tests.yield();
        }
    }
}

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
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static io.aeron.CommonContext.IPC_MEDIA;
import static io.aeron.CommonContext.UDP_MEDIA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

public class SessionSpecificSubscriptionTest
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

    private final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Tests::onError)
        .dirDeleteOnStart(true)
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED));

    private final Aeron aeron = Aeron.connect();

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(aeron, driver);
        driver.context().deleteDirectory();
    }

    @Timeout(10)
    @ParameterizedTest
    @MethodSource("data")
    public void shouldSubscribeToSpecificSessionIdsAndWildcard(final ChannelUriStringBuilder channelBuilder)
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
    public void shouldNotSubscribeWithoutSpecificSession(final ChannelUriStringBuilder channelBuilder)
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

    private static void publishMessage(final UnsafeBuffer buffer, final Publication publication)
    {
        while (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0L)
        {
            Tests.yield();
        }
    }
}

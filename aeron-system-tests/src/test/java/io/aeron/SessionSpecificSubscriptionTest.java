/*
 * Copyright 2014-2020 Real Logic Limited.
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;

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
    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));

    private final String channelUriWithoutSessionId = new ChannelUriStringBuilder()
        .endpoint(ENDPOINT).media(UDP_MEDIA).build();
    private final String channelUriWithSessionIdOne = new ChannelUriStringBuilder()
        .endpoint(ENDPOINT).media(UDP_MEDIA).sessionId(SESSION_ID_1).build();
    private final String channelUriWithSessionIdTwo = new ChannelUriStringBuilder()
        .endpoint(ENDPOINT).media(UDP_MEDIA).sessionId(SESSION_ID_2).build();

    private final FragmentHandler handlerSessionIdOne =
        (buffer, offset, length, header) -> assertEquals(SESSION_ID_1, header.sessionId());
    private final FragmentHandler handlerSessionIdTwo =
        (buffer, offset, length, header) -> assertEquals(SESSION_ID_2, header.sessionId());

    private final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Throwable::printStackTrace)
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

    @Test
    @Timeout(10)
    public void shouldSubscribeToSpecificSessionIdsAndWildcard()
    {
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
                Thread.yield();
                Tests.checkInterruptStatus();
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

    @Test
    public void shouldNotSubscribeWithoutSpecificSession()
    {
        try (Subscription subscription = aeron.addSubscription(channelUriWithSessionIdOne, STREAM_ID);
            Publication publication = aeron.addExclusivePublication(channelUriWithSessionIdOne, STREAM_ID);
            Publication publicationWildcard = aeron.addExclusivePublication(channelUriWithoutSessionId, STREAM_ID);
            Publication publicationWrongSession = aeron.addExclusivePublication(channelUriWithSessionIdTwo, STREAM_ID))
        {
            while (!publication.isConnected())
            {
                Thread.yield();
                Tests.checkInterruptStatus();
            }

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
            Thread.yield();
            Tests.checkInterruptStatus();
        }
    }
}

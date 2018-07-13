/*
 * Copyright 2014-2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class SessionSpecificSubscriptionTest
{
    private static final int SESSION_ID_1 = 1077;
    private static final int SESSION_ID_2 = 1078;
    private static final int STREAM_ID = 7;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = 1024 - DataHeaderFlyweight.HEADER_LENGTH;
    private static final int EXPECTED_NUMBER_OF_MESSAGES = 10;

    private final FragmentHandler mockFragmentHandler = mock(FragmentHandler.class);
    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));

    private final String channelUriWithoutSessionId = new ChannelUriStringBuilder()
        .endpoint("localhost:54325").media("udp").build();
    private final String channelUriWithSessionId1 = new ChannelUriStringBuilder()
        .endpoint("localhost:54325").media("udp").sessionId(SESSION_ID_1).build();
    private final String channelUriWithSessionId2 = new ChannelUriStringBuilder()
        .endpoint("localhost:54325").media("udp").sessionId(SESSION_ID_2).build();

    private final FragmentHandler handlerSessionId1 =
        (buffer, offset, length, header) -> assertThat(header.sessionId(), is(SESSION_ID_1));
    private final FragmentHandler handlerSessionId2 =
        (buffer, offset, length, header) -> assertThat(header.sessionId(), is(SESSION_ID_2));

    private final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Throwable::printStackTrace)
        .threadingMode(ThreadingMode.SHARED));

    private final Aeron aeron = Aeron.connect();

    @After
    public void after()
    {
        CloseHelper.close(aeron);
        CloseHelper.close(driver);
        driver.context().deleteAeronDirectory();
    }

    @Test(timeout = 10_000)
    public void shouldSubscribeToSpecificSessionIdsAndWildcard()
    {
        try (Subscription subscription1 = aeron.addSubscription(channelUriWithSessionId1, STREAM_ID);
            Subscription subscription2 = aeron.addSubscription(channelUriWithSessionId2, STREAM_ID);
            Subscription subscriptionWildcard = aeron.addSubscription(channelUriWithoutSessionId, STREAM_ID);
            ExclusivePublication publication1 = aeron.addExclusivePublication(channelUriWithSessionId1, STREAM_ID);
            ExclusivePublication publication2 = aeron.addExclusivePublication(channelUriWithSessionId2, STREAM_ID))
        {
            while (!publication1.isConnected() || !publication2.isConnected())
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            for (int i = 0; i < EXPECTED_NUMBER_OF_MESSAGES; i++)
            {
                publishMessage(srcBuffer, publication1);
                publishMessage(srcBuffer, publication2);
            }

            int numFragments = 0;
            do
            {
                SystemTest.checkInterruptedStatus();
                numFragments += subscription1.poll(handlerSessionId1, FRAGMENT_COUNT_LIMIT);
            }
            while (numFragments < EXPECTED_NUMBER_OF_MESSAGES);

            numFragments = 0;
            do
            {
                SystemTest.checkInterruptedStatus();
                numFragments += subscription2.poll(handlerSessionId2, FRAGMENT_COUNT_LIMIT);
            }
            while (numFragments < EXPECTED_NUMBER_OF_MESSAGES);

            numFragments = 0;
            do
            {
                SystemTest.checkInterruptedStatus();
                numFragments += subscriptionWildcard.poll(mockFragmentHandler, FRAGMENT_COUNT_LIMIT);
            }
            while (numFragments < (EXPECTED_NUMBER_OF_MESSAGES * 2));
        }
    }

    private static void publishMessage(final UnsafeBuffer buffer, final Publication publication)
    {
        while (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0L)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }
}

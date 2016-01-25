/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(Theories.class)
public class AbortedMessageTest
{
    @DataPoint
    public static final String UNICAST_CHANNEL = "aeron:udp?remote=localhost:54325";

    @DataPoint
    public static final String IPC_CHANNEL = CommonContext.IPC_CHANNEL;

    public static final int STREAM_ID = 1;
    public static final int FRAGMENT_COUNT_LIMIT = 10;
    public static final int MESSAGE_LENGTH = 200;

    private final FragmentHandler mockFragmentHandler = mock(FragmentHandler.class);

    @Theory
    @Test(timeout = 10000)
    public void shouldReceivePublishedMessage(final String channel) throws Exception
    {
        final BufferClaim bufferClaim = new BufferClaim();
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
        final MediaDriver.Context ctx = new MediaDriver.Context();

        try (final MediaDriver ignore = MediaDriver.launch(ctx);
             final Aeron aeron = Aeron.connect();
             final Publication publication = aeron.addPublication(channel, STREAM_ID);
             final Subscription subscription = aeron.addSubscription(channel, STREAM_ID))
        {
            publishMessage(srcBuffer, publication);

            while (publication.tryClaim(MESSAGE_LENGTH, bufferClaim) < 0L)
            {
                Thread.yield();
            }

            publishMessage(srcBuffer, publication);

            bufferClaim.abort();

            final int expectedNumberOfFragments = 2;
            int numFragments = 0;
            do
            {
                numFragments += subscription.poll(mockFragmentHandler, FRAGMENT_COUNT_LIMIT);
            }
            while (numFragments < expectedNumberOfFragments);

            verify(mockFragmentHandler, times(expectedNumberOfFragments)).onFragment(
                any(DirectBuffer.class), anyInt(), eq(MESSAGE_LENGTH), any(Header.class));
        }
        finally
        {
            ctx.deleteAeronDirectory();
        }
    }

    public static void publishMessage(final UnsafeBuffer srcBuffer, final Publication publication)
    {
        while (publication.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
        {
            Thread.yield();
        }
    }
}

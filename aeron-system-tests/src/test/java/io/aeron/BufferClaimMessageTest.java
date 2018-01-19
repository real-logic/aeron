/*
 * Copyright 2014-2017 Real Logic Ltd.
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
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

@RunWith(Theories.class)
public class BufferClaimMessageTest
{
    @DataPoint
    public static final String UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:54325";

    @DataPoint
    public static final String IPC_CHANNEL = CommonContext.IPC_CHANNEL;

    public static final int STREAM_ID = 1;
    public static final int FRAGMENT_COUNT_LIMIT = 10;
    public static final int MESSAGE_LENGTH = 200;

    private final FragmentHandler mockFragmentHandler = mock(FragmentHandler.class);

    @Theory
    @Test(timeout = 10000)
    public void shouldReceivePublishedMessageWithInterleavedAbort(final String channel)
    {
        final BufferClaim bufferClaim = new BufferClaim();
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .errorHandler(Throwable::printStackTrace)
            .threadingMode(ThreadingMode.SHARED);

        try (MediaDriver ignore = MediaDriver.launch(ctx);
            Aeron aeron = Aeron.connect();
            Publication publication = aeron.addPublication(channel, STREAM_ID);
            Subscription subscription = aeron.addSubscription(channel, STREAM_ID))
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
                final int fragments = subscription.poll(mockFragmentHandler, FRAGMENT_COUNT_LIMIT);
                if (0 == fragments)
                {
                    Thread.yield();
                }

                numFragments += fragments;
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

    @Theory
    @Test(timeout = 10000)
    public void shouldTransferReservedValue(final String channel)
    {
        final BufferClaim bufferClaim = new BufferClaim();
        final MediaDriver.Context ctx = new MediaDriver.Context();

        try (MediaDriver ignore = MediaDriver.launch(ctx);
            Aeron aeron = Aeron.connect();
            Publication publication = aeron.addPublication(channel, STREAM_ID);
            Subscription subscription = aeron.addSubscription(channel, STREAM_ID))
        {
            while (publication.tryClaim(MESSAGE_LENGTH, bufferClaim) < 0L)
            {
                Thread.yield();
            }

            final long reservedValue = System.currentTimeMillis();
            bufferClaim.reservedValue(reservedValue);
            bufferClaim.commit();

            final boolean[] done = new boolean[1];
            while (!done[0])
            {
                subscription.poll(
                    (buffer, offset, length, header) ->
                    {
                        assertThat(length, is(MESSAGE_LENGTH));
                        assertThat(header.reservedValue(), is(reservedValue));

                        done[0] = true;
                    },
                    FRAGMENT_COUNT_LIMIT);
            }
        }
        finally
        {
            ctx.deleteAeronDirectory();
        }
    }

    private static void publishMessage(final UnsafeBuffer srcBuffer, final Publication publication)
    {
        while (publication.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
        {
            Thread.yield();
        }
    }
}

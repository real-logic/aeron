/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableInteger;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Theories.class)
public class BufferClaimMessageTest
{
    @DataPoint
    public static final String UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:54325";

    @DataPoint
    public static final String IPC_CHANNEL = CommonContext.IPC_CHANNEL;

    private static final int STREAM_ID = 1;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = 200;

    private final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Throwable::printStackTrace)
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED));

    private final Aeron aeron = Aeron.connect();

    @After
    public void after()
    {
        CloseHelper.close(aeron);
        CloseHelper.close(driver);
        driver.context().deleteAeronDirectory();
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldReceivePublishedMessageWithInterleavedAbort(final String channel)
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
                SystemTest.checkInterruptedStatus();
                Thread.yield();
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
                    SystemTest.checkInterruptedStatus();
                    Thread.yield();
                }

                numFragments += fragments;
            }
            while (numFragments < expectedNumberOfFragments);

            assertThat(fragmentCount.value, is(expectedNumberOfFragments));
        }
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldTransferReservedValue(final String channel)
    {
        final BufferClaim bufferClaim = new BufferClaim();

        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            Publication publication = aeron.addPublication(channel, STREAM_ID))
        {
            while (publication.tryClaim(MESSAGE_LENGTH, bufferClaim) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            final long reservedValue = System.currentTimeMillis();
            bufferClaim.reservedValue(reservedValue);
            bufferClaim.commit();

            final MutableBoolean done = new MutableBoolean();
            while (!done.get())
            {
                final int fragments = subscription.poll(
                    (buffer, offset, length, header) ->
                    {
                        assertThat(length, is(MESSAGE_LENGTH));
                        assertThat(header.reservedValue(), is(reservedValue));

                        done.value = true;
                    },
                    FRAGMENT_COUNT_LIMIT);

                if (0 == fragments)
                {
                    SystemTest.checkInterruptedStatus();
                    Thread.yield();
                }
            }
        }
    }

    private static void publishMessage(final UnsafeBuffer srcBuffer, final Publication publication)
    {
        while (publication.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }
}

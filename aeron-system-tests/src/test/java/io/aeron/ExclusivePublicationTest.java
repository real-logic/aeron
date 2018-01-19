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
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

@RunWith(Theories.class)
public class ExclusivePublicationTest
{
    @DataPoint
    public static final String MULTICAST_CHANNEL = "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost";

    @DataPoint
    public static final String UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:54325";

    @DataPoint
    public static final String IPC_CHANNEL = CommonContext.IPC_CHANNEL;

    public static final int STREAM_ID = 7;
    public static final int FRAGMENT_COUNT_LIMIT = 10;
    public static final int MESSAGE_LENGTH = 200;

    private final FragmentHandler mockFragmentHandler = mock(FragmentHandler.class);
    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));

    @Theory
    @Test(timeout = 10000)
    public void shouldPublishFromIndependentExclusivePublications(final String channel)
    {
        final AtomicInteger imageCounter = new AtomicInteger();
        final AvailableImageHandler availableImageHandler = (image) -> imageCounter.getAndIncrement();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .errorHandler(Throwable::printStackTrace)
            .threadingMode(ThreadingMode.SHARED);

        final Aeron.Context clientCtx = new Aeron.Context().availableImageHandler(availableImageHandler);

        try (MediaDriver ignore = MediaDriver.launch(driverCtx);
            Aeron aeron = Aeron.connect(clientCtx);
            ExclusivePublication publicationOne = aeron.addExclusivePublication(channel, STREAM_ID);
            ExclusivePublication publicationTwo = aeron.addExclusivePublication(channel, STREAM_ID);
            Subscription subscription = aeron.addSubscription(channel, STREAM_ID))
        {
            final int expectedNumberOfFragments = 778;

            for (int i = 0; i < expectedNumberOfFragments; i += 2)
            {
                publishMessage(srcBuffer, publicationOne);
                publishMessage(srcBuffer, publicationTwo);
            }

            int totalFragmentsRead = 0;
            do
            {
                final int fragmentsRead = subscription.poll(mockFragmentHandler, FRAGMENT_COUNT_LIMIT);
                if (0 == fragmentsRead)
                {
                    Thread.yield();
                }

                totalFragmentsRead += fragmentsRead;
            }
            while (totalFragmentsRead < expectedNumberOfFragments);

            verify(mockFragmentHandler, times(expectedNumberOfFragments)).onFragment(
                any(DirectBuffer.class), anyInt(), eq(MESSAGE_LENGTH), any(Header.class));

            assertThat(imageCounter.get(), is(2));
        }
        finally
        {
            driverCtx.deleteAeronDirectory();
        }
    }

    private static void publishMessage(final UnsafeBuffer srcBuffer, final ExclusivePublication publication)
    {
        while (publication.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
        {
            Thread.yield();
        }
    }
}

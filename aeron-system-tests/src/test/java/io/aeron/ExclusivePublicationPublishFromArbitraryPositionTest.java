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
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.mockito.Mockito.*;

public class ExclusivePublicationPublishFromArbitraryPositionTest
{
    @Rule
    public TestWatcher testWatcher = new TestWatcher()
    {
        protected void failed(final Throwable t, final Description description)
        {
            System.err.println("ExclusivePublicationPublishFromArbitraryPositionTest failed with random seed: " + seed);
        }
    };

    public static final int STREAM_ID = 7;
    public static final int FRAGMENT_COUNT_LIMIT = 10;
    public static final int MAX_MESSAGE_LENGTH = 1024 - DataHeaderFlyweight.HEADER_LENGTH;

    private final FragmentHandler mockFragmentHandler = mock(FragmentHandler.class);
    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MAX_MESSAGE_LENGTH));
    private long seed;

    @Test(timeout = 10000)
    @Ignore
    public void shouldPublishFromArbitraryJoinPosition() throws Exception
    {
        final Random rnd = new Random();
        seed = System.nanoTime();
        rnd.setSeed(seed);
        final int initialTermId = rnd.nextInt(1234);
        final int termLength = 1 << (16 + rnd.nextInt(10)); // 64k to 64M
        final int termOffset = BitUtil.align(rnd.nextInt(termLength), FrameDescriptor.FRAME_ALIGNMENT);
        // This test passes when termId=initialTermId
        final int termId = initialTermId + rnd.nextInt(1000);
        final ChannelUriBuilder builder = new ChannelUriBuilder()
            .endpoint("localhost:54325")
            .termLength(termLength)
            .initialTermId(initialTermId)
            .termId(termId)
            .termOffset(termOffset)
            .mtu(1 << (10 + rnd.nextInt(3))) // 1024 to 8096
            .media("udp");
        final String publishUri = builder.buildUri();
        final int expectedNumberOfFragments = 10 + rnd.nextInt(10000);


        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .termBufferSparseFile(true);

        try (MediaDriver ignore = MediaDriver.launch(driverCtx);
             Aeron aeron = Aeron.connect();
             ExclusivePublication publicationOne = aeron.addExclusivePublication(publishUri, STREAM_ID);
             Subscription subscription = aeron.addSubscription(publishUri, STREAM_ID))
        {
            while (!publicationOne.isConnected())
            {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }

            final Thread t = new Thread(
                () ->
                {
                    int totalFragmentsRead = 0;
                    do
                    {
                        int fragmentsRead = subscription.poll(mockFragmentHandler, FRAGMENT_COUNT_LIMIT);
                        while (0 == fragmentsRead)
                        {
                            Thread.yield();
                            fragmentsRead = subscription.poll(mockFragmentHandler, FRAGMENT_COUNT_LIMIT);
                        }

                        totalFragmentsRead += fragmentsRead;
                    }
                    while (totalFragmentsRead < expectedNumberOfFragments);

                    verify(mockFragmentHandler, times(expectedNumberOfFragments)).onFragment(
                        any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class));
                });

            t.setDaemon(false);
            t.setName("image-consumer");
            t.start();

            for (int i = 0; i < expectedNumberOfFragments; i++)
            {
                publishMessage(srcBuffer, publicationOne, rnd);
            }

            t.join();
        }
        finally
        {
            driverCtx.deleteAeronDirectory();
        }
    }

    private static void publishMessage(
        final UnsafeBuffer srcBuffer,
        final ExclusivePublication publication,
        final Random rnd)
    {
        while (publication.offer(srcBuffer, 0, 1 + rnd.nextInt(MAX_MESSAGE_LENGTH - 1)) < 0L)
        {
            Thread.yield();
        }
    }
}

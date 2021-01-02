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
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.Tests;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestWatcher;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class PublishFromArbitraryPositionTest
{
    private static final int STREAM_ID = 1007;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MAX_MESSAGE_LENGTH = 1024 - DataHeaderFlyweight.HEADER_LENGTH;

    private final FragmentHandler mockFragmentHandler = mock(FragmentHandler.class);
    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MAX_MESSAGE_LENGTH));
    private final long seed = System.nanoTime();

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    private final TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Tests::onError)
        .threadingMode(ThreadingMode.SHARED),
        testWatcher);

    private final Aeron aeron = Aeron.connect();

    @RegisterExtension
    public final TestWatcher randomSeedWatcher = new TestWatcher()
    {
        public void testFailed(final ExtensionContext context, final Throwable cause)
        {
            System.err.println(context.getDisplayName() + " failed with random seed: " + seed);
        }
    };

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(aeron, driver);
        driver.context().deleteDirectory();
    }

    @Test
    @Timeout(10)
    public void shouldPublishFromArbitraryJoinPosition() throws InterruptedException
    {
        final Random rnd = new Random();
        rnd.setSeed(seed);

        final int termLength = 1 << (16 + rnd.nextInt(10)); // 64k to 64M
        final int mtu = 1 << (10 + rnd.nextInt(3)); // 1024 to 8096
        final int initialTermId = rnd.nextInt(1234);
        final int termOffset = BitUtil.align(rnd.nextInt(termLength), FrameDescriptor.FRAME_ALIGNMENT);
        final int termId = initialTermId + rnd.nextInt(1000);
        final String channelUri = new ChannelUriStringBuilder()
            .endpoint("localhost:24325")
            .termLength(termLength)
            .initialTermId(initialTermId)
            .termId(termId)
            .termOffset(termOffset)
            .mtu(mtu)
            .media("udp")
            .build();

        final int expectedNumberOfFragments = 10 + rnd.nextInt(10000);

        try (Subscription subscription = aeron.addSubscription(channelUri, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(channelUri, STREAM_ID))
        {
            Tests.awaitConnected(publication);

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

                    assertEquals(expectedNumberOfFragments, totalFragmentsRead);
                });

            t.setDaemon(true);
            t.setName("image-consumer");
            t.start();

            for (int i = 0; i < expectedNumberOfFragments; i++)
            {
                publishMessage(srcBuffer, publication, rnd);
            }

            t.join();
        }
    }

    private static void publishMessage(
        final UnsafeBuffer buffer, final ExclusivePublication publication, final Random rnd)
    {
        while (publication.offer(buffer, 0, 1 + rnd.nextInt(MAX_MESSAGE_LENGTH - 1)) < 0L)
        {
            Tests.yield();
        }
    }
}

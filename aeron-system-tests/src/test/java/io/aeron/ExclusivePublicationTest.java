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
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Theories.class)
public class ExclusivePublicationTest
{
    @DataPoint
    public static final String MULTICAST_CHANNEL = "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost";

    @DataPoint
    public static final String UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:54325";

    @DataPoint
    public static final String IPC_CHANNEL = CommonContext.IPC_CHANNEL;

    private static final int STREAM_ID = 7;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = 200;

    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));

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

    @Theory
    @Test(timeout = 10_000)
    public void shouldPublishFromIndependentExclusivePublications(final String channel)
    {
        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publicationOne = aeron.addExclusivePublication(channel, STREAM_ID);
            ExclusivePublication publicationTwo = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            while (subscription.imageCount() < 2)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            final int expectedNumberOfFragments = 778;

            for (int i = 0; i < expectedNumberOfFragments; i += 2)
            {
                publishMessage(srcBuffer, publicationOne);
                publishMessage(srcBuffer, publicationTwo);
            }

            final MutableInteger messageCount = new MutableInteger();
            int totalFragmentsRead = 0;
            do
            {
                final int fragmentsRead = subscription.poll(
                    (buffer, offset, length, header) ->
                    {
                        assertThat(length, is(MESSAGE_LENGTH));
                        messageCount.value++;
                    },
                    FRAGMENT_COUNT_LIMIT);

                if (0 == fragmentsRead)
                {
                    SystemTest.checkInterruptedStatus();
                    Thread.yield();
                }

                totalFragmentsRead += fragmentsRead;
            }
            while (totalFragmentsRead < expectedNumberOfFragments);

            assertThat(messageCount.value, is(expectedNumberOfFragments));
        }
    }

    private static void publishMessage(final UnsafeBuffer srcBuffer, final ExclusivePublication publication)
    {
        while (publication.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }
}

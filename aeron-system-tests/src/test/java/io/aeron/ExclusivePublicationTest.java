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
import io.aeron.test.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.List;

import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

public class ExclusivePublicationTest
{
    private static List<String> channels()
    {
        return asList(
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost",
            "aeron:udp?endpoint=localhost:54325",
            CommonContext.IPC_CHANNEL
        );
    }

    private static final int STREAM_ID = 1007;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = 200;

    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));

    private final TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Throwable::printStackTrace)
        .dirDeleteOnShutdown(true)
        .threadingMode(ThreadingMode.SHARED));

    private final Aeron aeron = Aeron.connect();

    @AfterEach
    public void after()
    {
        CloseHelper.close(aeron);
        CloseHelper.close(driver);
    }

    @ParameterizedTest
    @MethodSource("channels")
    public void shouldPublishFromIndependentExclusivePublications(final String channel)
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
                ExclusivePublication publicationOne = aeron.addExclusivePublication(channel, STREAM_ID);
                ExclusivePublication publicationTwo = aeron.addExclusivePublication(channel, STREAM_ID))
            {
                while (subscription.imageCount() < 2)
                {
                    Thread.yield();
                    SystemTest.checkInterruptedStatus();
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
                            assertEquals(MESSAGE_LENGTH, length);
                            messageCount.value++;
                        },
                        FRAGMENT_COUNT_LIMIT);

                    if (0 == fragmentsRead)
                    {
                        Thread.yield();
                        SystemTest.checkInterruptedStatus();
                    }

                    totalFragmentsRead += fragmentsRead;
                }
                while (totalFragmentsRead < expectedNumberOfFragments);

                assertEquals(expectedNumberOfFragments, messageCount.value);
            }
        });
    }

    private static void publishMessage(final UnsafeBuffer srcBuffer, final ExclusivePublication publication)
    {
        while (publication.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
        {
            Thread.yield();
            SystemTest.checkInterruptedStatus();
        }
    }
}

/*
 * Copyright 2014-2025 Real Logic Limited.
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
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(InterruptingTestCallback.class)
class MdsEosPositionTest
{
    private static final long INITIAL_STREAM_POSITION = 0;
    public static final int SESSION_ID = 555;
    private static final int INITIAL_TERM_ID = 777;
    private static final int TERM_LENGTH = 64 * 1024;
    private static final int MESSAGE_COUNT = 10;
    private static final int STREAM_ID = 1001;
    private static final int MESSAGE_LENGTH = 200;
    private static final String ENDPOINT_ONE = "localhost:3333";
    private static final String ENDPOINT_TWO = "localhost:6666";

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    @Test
    @SlowTest
    @InterruptAfter(30)
    void shouldHaveEosPositionAtFinalPosition()
    {
        final ChannelUriStringBuilder uriBuilder = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .sessionId(SESSION_ID)
            .initialPosition(INITIAL_STREAM_POSITION, INITIAL_TERM_ID, TERM_LENGTH);

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .errorHandler(Tests::onError);

        try (TestMediaDriver mediaDriver = TestMediaDriver.launch(driverCtx, testWatcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())))
        {
            testWatcher.dataCollector().add(mediaDriver.context().aeronDirectory());

            final Subscription subscription = aeron.addSubscription("aeron:udp?control-mode=manual", STREAM_ID);
            subscription.addDestination("aeron:udp?endpoint=" + ENDPOINT_ONE);
            subscription.addDestination("aeron:udp?endpoint=" + ENDPOINT_TWO);

            final Publication publicationOne = aeron.addPublication(
                uriBuilder.endpoint(ENDPOINT_ONE).build(), STREAM_ID);
            final Publication publicationTwo = aeron.addPublication(
                uriBuilder.endpoint(ENDPOINT_TWO).build(), STREAM_ID);

            Tests.awaitConnected(subscription);

            publishStream(publicationTwo);

            final long expectedPosition = publicationTwo.position();
            final Image image = subscription.imageAtIndex(0);

            consumeStream(image);

            awaitActiveTransportCount(image, 2);

            publicationTwo.close();

            awaitActiveTransportCount(image, 1);

            publicationOne.close();

            awaitClosed(image);

            assertTrue(image.isEndOfStream());
            assertEquals(expectedPosition, image.position());
            assertEquals(expectedPosition, image.endOfStreamPosition());
        }
    }

    private static void publishStream(final Publication publicationTwo)
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));
        for (int i = 0; i < MESSAGE_COUNT; i++)
        {
            srcBuffer.putStringAscii(0, Integer.toString(i));
            while (publicationTwo.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
            {
                Tests.yield();
            }
        }
    }

    private static void consumeStream(final Image image)
    {
        int i = 0;
        while (i++ < MESSAGE_COUNT)
        {
            while (0 == image.poll((buffer, offset, length, header) -> {}, 1))
            {
                Tests.yield();
            }
        }
    }

    private static void awaitActiveTransportCount(final Image image, final int activeTransportCount)
    {
        while (image.activeTransportCount() != activeTransportCount)
        {
            Tests.sleep(1);
        }
    }

    private static void awaitClosed(final Image image)
    {
        while (!image.isClosed())
        {
            Tests.sleep(1);
        }
    }
}

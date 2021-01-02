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
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.Tests;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.ByteBuffer;

import static io.aeron.Publication.MAX_POSITION_EXCEEDED;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MaxPositionPublicationTest
{
    private static final int STREAM_ID = 1007;
    private static final int MESSAGE_LENGTH = 32;

    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    private final TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Tests::onError)
        .dirDeleteOnStart(true)
        .threadingMode(ThreadingMode.SHARED),
        testWatcher);

    private final Aeron aeron = Aeron.connect();

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(aeron, driver);
        driver.context().deleteDirectory();
    }

    @Test
    @Timeout(10)
    public void shouldPublishFromExclusivePublication()
    {
        final int initialTermId = -777;
        final int termLength = 64 * 1024;
        final long maxPosition = termLength * (Integer.MAX_VALUE + 1L);
        final long lastMessagePosition = maxPosition - (MESSAGE_LENGTH + DataHeaderFlyweight.HEADER_LENGTH);

        final String channelUri = new ChannelUriStringBuilder()
            .initialPosition(lastMessagePosition, initialTermId, termLength)
            .media("ipc")
            .validate()
            .build();

        try (Subscription subscription = aeron.addSubscription(channelUri, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(channelUri, STREAM_ID))
        {
            Tests.awaitConnected(subscription);

            assertEquals(lastMessagePosition, publication.position());
            assertEquals(lastMessagePosition, subscription.imageAtIndex(0).joinPosition());

            long resultingPosition = publication.offer(srcBuffer, 0, MESSAGE_LENGTH);
            while (resultingPosition < 0)
            {
                Tests.yield();
                resultingPosition = publication.offer(srcBuffer, 0, MESSAGE_LENGTH);
            }

            assertEquals(maxPosition, publication.maxPossiblePosition());
            assertEquals(publication.maxPossiblePosition(), resultingPosition);
            assertEquals(MAX_POSITION_EXCEEDED, publication.offer(srcBuffer, 0, MESSAGE_LENGTH));
            assertEquals(MAX_POSITION_EXCEEDED, publication.offer(srcBuffer, 0, MESSAGE_LENGTH));
        }
    }
}

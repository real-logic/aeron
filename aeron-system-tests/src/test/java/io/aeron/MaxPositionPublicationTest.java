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
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.aeron.Publication.MAX_POSITION_EXCEEDED;
import static org.junit.Assert.assertEquals;

public class MaxPositionPublicationTest
{
    public static final int STREAM_ID = 7;
    public static final int FRAGMENT_COUNT_LIMIT = 10;
    public static final int MESSAGE_LENGTH = 32;

    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));

    @Test(timeout = 10000)
    @SuppressWarnings("unused")
    public void shouldPublishFromIndependentExclusivePublications()
    {
        final int termLength = 64 * 1024;
        final String channelUri = new ChannelUriStringBuilder()
            .termLength(termLength)
            .initialTermId(-777)
            .termId(-777 + Integer.MAX_VALUE)
            .termOffset(termLength - (MESSAGE_LENGTH + DataHeaderFlyweight.HEADER_LENGTH))
            .media("ipc")
            .validate()
            .build();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .errorHandler(Throwable::printStackTrace);

        try (MediaDriver ignore = MediaDriver.launch(driverCtx);
            Aeron aeron = Aeron.connect();
            ExclusivePublication publication = aeron.addExclusivePublication(channelUri, STREAM_ID);
            Subscription subscription = aeron.addSubscription(channelUri, STREAM_ID))
        {
            long resultingPosition = publication.offer(srcBuffer, 0, MESSAGE_LENGTH);
            while (resultingPosition < 0)
            {
                Thread.yield();
                resultingPosition = publication.offer(srcBuffer, 0, MESSAGE_LENGTH);
            }

            assertEquals(publication.maxPossiblePosition(), resultingPosition);
            assertEquals(MAX_POSITION_EXCEEDED, publication.offer(srcBuffer, 0, MESSAGE_LENGTH));
            assertEquals(MAX_POSITION_EXCEEDED, publication.offer(srcBuffer, 0, MESSAGE_LENGTH));
        }
        finally
        {
            driverCtx.deleteAeronDirectory();
        }
    }
}

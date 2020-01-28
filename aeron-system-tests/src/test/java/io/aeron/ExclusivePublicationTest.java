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
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.Publication.CLOSED;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.junit.jupiter.api.Assertions.*;

public class ExclusivePublicationTest
{
    private static List<String> channels()
    {
        return asList(
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost",
            "aeron:udp?endpoint=localhost:24325",
            IPC_CHANNEL);
    }

    private static final int STREAM_ID = 1007;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = 200;

    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(allocateDirectAligned(100 * 1024 * 1024, 64));

    private final TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Throwable::printStackTrace)
        .dirDeleteOnShutdown(true)
        .threadingMode(ThreadingMode.SHARED));

    private final Aeron aeron = Aeron.connect();

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(aeron, driver);
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
                awaitPublications(subscription, 2);

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
                        Tests.checkInterruptedStatus();
                    }

                    totalFragmentsRead += fragmentsRead;
                }
                while (totalFragmentsRead < expectedNumberOfFragments);

                assertEquals(expectedNumberOfFragments, messageCount.value);
            }
        });
    }

    @Test
    void offerBlockThrowsIllegalArgumentExceptionIfLengthExceedsAvailableSpaceWithinTheTerm()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            try (Subscription subscription = aeron.addSubscription(IPC_CHANNEL, STREAM_ID);
                ExclusivePublication publication = aeron.addExclusivePublication(IPC_CHANNEL, STREAM_ID))
            {
                awaitPublications(subscription, 1);

                publishMessage(srcBuffer, publication);

                final int termBufferLength = driver.context().ipcTermBufferLength();
                final int termOffset = publication.termOffset();
                final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                    () -> publication.offerBlock(srcBuffer, 0, termBufferLength));

                assertEquals("invalid block length=" + termBufferLength + ", available space in the term buffer=" +
                    (termBufferLength - termOffset), exception.getMessage());
            }
        });
    }

    @Test
    void offerBlockReturnsClosedWhenPublicationIsClosed()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            try (Subscription subscription = aeron.addSubscription(IPC_CHANNEL, STREAM_ID);
                ExclusivePublication publication = aeron.addExclusivePublication(IPC_CHANNEL, STREAM_ID))
            {
                awaitPublications(subscription, 1);
                publication.close();

                final long result = publication.offerBlock(srcBuffer, 0, MESSAGE_LENGTH);

                assertEquals(CLOSED, result);
            }
        });
    }

    @Test
    void offerBlockThrowsIllegalArgumentExceptionUponInvalidFirstFrame()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            try (Subscription subscription = aeron.addSubscription(IPC_CHANNEL, STREAM_ID);
                ExclusivePublication publication = aeron.addExclusivePublication(IPC_CHANNEL, STREAM_ID))
            {
                awaitPublications(subscription, 1);
                final int sessionId = publication.sessionId();
                final int streamId = publication.streamId();
                final int offset = 128;
                frameType(srcBuffer, offset, HDR_TYPE_NAK);
                frameSessionId(srcBuffer, offset, -19);
                srcBuffer.putInt(offset + STREAM_ID_FIELD_OFFSET, 42, LITTLE_ENDIAN);

                final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                    () -> publication.offerBlock(srcBuffer, offset, 1000));

                assertEquals("improperly formatted block of messages: sessionId=-19" +
                    " (expected=" + sessionId + "), streamId=42 (expected=" + streamId +
                    "), frameType=" + HDR_TYPE_NAK + " (expected=" + HDR_TYPE_DATA + ")", exception.getMessage());
            }
        });
    }

    @Test
    void offerBlockAcceptsUpToAnEntireTermOfData()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            try (Subscription subscription = aeron.addSubscription(IPC_CHANNEL, STREAM_ID);
                ExclusivePublication publication = aeron.addExclusivePublication(IPC_CHANNEL, STREAM_ID))
            {
                awaitPublications(subscription, 1);
                final int sessionId = publication.sessionId();
                final int streamId = publication.streamId();
                final int currentTermId = publication.termId();
                final int termBufferLength = driver.context().ipcTermBufferLength();
                final int dataLength = termBufferLength - HEADER_LENGTH;
                final int offset = 1024;
                frameType(srcBuffer, offset, HDR_TYPE_DATA);
                frameLengthOrdered(srcBuffer, offset, dataLength);
                frameSessionId(srcBuffer, offset, sessionId);
                srcBuffer.putInt(offset + STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);
                srcBuffer.setMemory(offset + DATA_OFFSET, dataLength, (byte)13);
                final long position = publication.position();

                final long result = publication.offerBlock(srcBuffer, offset, termBufferLength);

                assertEquals(position + termBufferLength, result);
                final long pollBytes = subscription.rawPoll(
                    (fileChannel, fileOffset, termBuffer, termOffset, length, pollSessionId, termId) ->
                    {
                        assertEquals(HDR_TYPE_DATA, frameType(termBuffer, termOffset));
                        assertEquals(dataLength, frameLength(termBuffer, termOffset));
                        assertEquals(sessionId, frameSessionId(termBuffer, termOffset));
                        assertEquals(streamId,
                            termBuffer.getInt(termOffset + STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN));
                    }, termBufferLength);
                assertEquals(dataLength, pollBytes);
                assertEquals(publication.termBufferLength(), publication.termOffset());
                assertEquals(currentTermId, publication.termId());
            }
        });
    }

    @Test
    void offerBlockReturnsBackpressureStatus()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            try (Subscription subscription = aeron.addSubscription(IPC_CHANNEL, STREAM_ID);
                ExclusivePublication publication = aeron.addExclusivePublication(IPC_CHANNEL, STREAM_ID))
            {
                awaitPublications(subscription, 1);
                final int length = driver.context().ipcTermBufferLength() / 2;
                final int sessionId = publication.sessionId();
                final int streamId = publication.streamId();
                final int dataLength = length - HEADER_LENGTH;
                final int offset = 2048;
                frameType(srcBuffer, offset, HDR_TYPE_DATA);
                frameLengthOrdered(srcBuffer, offset, dataLength);
                frameSessionId(srcBuffer, offset, sessionId);
                srcBuffer.putInt(offset + STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);
                srcBuffer.setMemory(offset + DATA_OFFSET, dataLength, (byte)6);
                publication.offerBlock(srcBuffer, offset, length);

                final long result = publication.offerBlock(srcBuffer, 0, offset);

                assertEquals(BACK_PRESSURED, result);
            }
        });
    }

    private static void awaitPublications(final Subscription subscription, final int imageCount)
    {
        while (subscription.imageCount() < imageCount)
        {
            Thread.yield();
            Tests.checkInterruptedStatus();
        }
    }

    private static void publishMessage(final UnsafeBuffer srcBuffer, final ExclusivePublication publication)
    {
        while (publication.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
        {
            Thread.yield();
            Tests.checkInterruptedStatus();
        }
    }
}

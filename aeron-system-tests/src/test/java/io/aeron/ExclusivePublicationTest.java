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
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.RawBlockHandler;
import io.aeron.test.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.Publication.CLOSED;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExclusivePublicationTest
{
    private static List<String> channels()
    {
        return asList(
            "aeron:udp?endpoint=224.20.30.39:24323|interface=localhost|term-length=64k",
            "aeron:udp?endpoint=localhost:24325|term-length=64k",
            IPC_CHANNEL + "?term-length=64k");
    }

    private static final int STREAM_ID = 1007;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = 200;

    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[65 * 1024]);

    private final TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Throwable::printStackTrace)
        .threadingMode(ThreadingMode.SHARED));

    private final Aeron aeron = Aeron.connect();

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(aeron, driver);
        driver.context().deleteDirectory();
    }

    @ParameterizedTest
    @MethodSource("channels")
    @Timeout(10)
    public void shouldPublishFromIndependentExclusivePublications(final String channel)
    {
        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publicationOne = aeron.addExclusivePublication(channel, STREAM_ID);
            ExclusivePublication publicationTwo = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            awaitConnection(subscription, 2);

            final int expectedNumberOfFragments = 778;
            int totalFragmentsRead = 0;
            final MutableInteger messageCount = new MutableInteger();
            final FragmentHandler fragmentHandler =
                (buffer, offset, length, header) ->
                {
                    assertEquals(MESSAGE_LENGTH, length);
                    messageCount.value++;
                };

            for (int i = 0; i < expectedNumberOfFragments; i += 2)
            {
                publishMessage(srcBuffer, publicationOne);
                publishMessage(srcBuffer, publicationTwo);
                totalFragmentsRead += pollFragments(subscription, fragmentHandler);
            }

            do
            {
                totalFragmentsRead += pollFragments(subscription, fragmentHandler);
            }
            while (totalFragmentsRead < expectedNumberOfFragments);

            assertEquals(expectedNumberOfFragments, messageCount.value);
        }
    }

    @Test
    @Timeout(10)
    void offerBlockThrowsIllegalArgumentExceptionIfLengthExceedsAvailableSpaceWithinTheTerm()
    {
        try (Subscription subscription = aeron.addSubscription(IPC_CHANNEL, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(IPC_CHANNEL, STREAM_ID))
        {
            awaitConnection(subscription, 1);

            publishMessage(srcBuffer, publication);

            final int termBufferLength = publication.termBufferLength();
            final int termOffset = publication.termOffset();
            final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> publication.offerBlock(srcBuffer, 0, termBufferLength));

            assertEquals("invalid block length " + termBufferLength +
                ", remaining space in term " + (termBufferLength - termOffset), exception.getMessage());
        }
    }

    @Test
    @Timeout(10)
    void offerBlockReturnsClosedWhenPublicationIsClosed()
    {
        try (Subscription subscription = aeron.addSubscription(IPC_CHANNEL, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(IPC_CHANNEL, STREAM_ID))
        {
            awaitConnection(subscription, 1);
            publication.close();

            final long result = publication.offerBlock(srcBuffer, 0, MESSAGE_LENGTH);
            assertEquals(CLOSED, result);
        }
    }

    @Test
    @Timeout(10)
    void offerBlockThrowsIllegalArgumentExceptionUponInvalidFirstFrame()
    {
        try (Subscription subscription = aeron.addSubscription(IPC_CHANNEL, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(IPC_CHANNEL, STREAM_ID))
        {
            awaitConnection(subscription, 1);

            final int sessionId = publication.sessionId();
            final int streamId = publication.streamId();
            final int termId = publication.termId();
            final int offset = 128;

            frameType(srcBuffer, offset, HDR_TYPE_NAK);
            frameSessionId(srcBuffer, offset, -19);
            srcBuffer.putInt(offset + STREAM_ID_FIELD_OFFSET, 42, LITTLE_ENDIAN);

            final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> publication.offerBlock(srcBuffer, offset, 1000));

            assertEquals("improperly formatted block:" +
                " termOffset=0" + " (expected=0)," +
                " sessionId=-19" + " (expected=" + sessionId + ")," +
                " streamId=42 (expected=" + streamId + ")," +
                " termId=0 (expected=" + termId + ")," +
                " frameType=" + HDR_TYPE_NAK + " (expected=" + HDR_TYPE_DATA + ")",
                ex.getMessage());
        }
    }

    @Test
    @Timeout(10)
    void offerBlockAcceptsUpToAnEntireTermOfData()
    {
        final String channel = IPC_CHANNEL + "?term-length=64k";
        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            awaitConnection(subscription, 1);

            final int sessionId = publication.sessionId();
            final int streamId = publication.streamId();
            final int currentTermId = publication.termId();
            final int termBufferLength = publication.termBufferLength();
            final int offset = 1024;

            frameType(srcBuffer, offset, HDR_TYPE_DATA);
            frameLengthOrdered(srcBuffer, offset, termBufferLength);
            frameSessionId(srcBuffer, offset, sessionId);
            srcBuffer.putInt(offset + STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);
            srcBuffer.putInt(offset + TERM_ID_FIELD_OFFSET, currentTermId, LITTLE_ENDIAN);
            srcBuffer.setMemory(offset + DATA_OFFSET, termBufferLength - HEADER_LENGTH, (byte)13);

            final long position = publication.position();
            final long result = publication.offerBlock(srcBuffer, offset, termBufferLength);
            assertEquals(position + termBufferLength, result);

            final RawBlockHandler rawBlockHandler =
                (fileChannel, fileOffset, termBuffer, termOffset, length, pollSessionId, termId) ->
                {
                    assertEquals(HDR_TYPE_DATA, frameType(termBuffer, termOffset));
                    assertEquals(termBufferLength, frameLength(termBuffer, termOffset));
                    assertEquals(sessionId, frameSessionId(termBuffer, termOffset));
                    assertEquals(streamId, termBuffer.getInt(termOffset + STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN));
                };

            final long pollBytes = subscription.rawPoll(rawBlockHandler, termBufferLength);
            assertEquals(termBufferLength, pollBytes);
            assertEquals(publication.termBufferLength(), publication.termOffset());
            assertEquals(currentTermId, publication.termId());
        }
    }

    @Test
    @Timeout(10)
    void offerBlockReturnsBackPressuredStatus()
    {
        final String channel = IPC_CHANNEL + "?term-length=64k";
        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            awaitConnection(subscription, 1);

            final int length = publication.termBufferLength() / 2;
            final int sessionId = publication.sessionId();
            final int streamId = publication.streamId();
            final int termId = publication.termId();
            final int dataLength = length - HEADER_LENGTH;
            final int offset = 2048;

            frameType(srcBuffer, offset, HDR_TYPE_DATA);
            frameLengthOrdered(srcBuffer, offset, dataLength);
            frameSessionId(srcBuffer, offset, sessionId);
            srcBuffer.putInt(offset + STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);
            srcBuffer.putInt(offset + TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);
            srcBuffer.setMemory(offset + DATA_OFFSET, dataLength, (byte)6);
            publication.offerBlock(srcBuffer, offset, length);

            final long result = publication.offerBlock(srcBuffer, 0, offset);

            assertEquals(BACK_PRESSURED, result);
        }
    }

    private static void awaitConnection(final Subscription subscription, final int imageCount)
    {
        while (subscription.imageCount() < imageCount)
        {
            Thread.yield();
            Tests.checkInterruptStatus();
        }
    }

    private static void publishMessage(final UnsafeBuffer srcBuffer, final ExclusivePublication publication)
    {
        while (publication.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
        {
            Thread.yield();
            Tests.checkInterruptStatus();
        }
    }

    private int pollFragments(final Subscription subscription, final FragmentHandler fragmentHandler)
    {
        final int fragmentsRead = subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
        if (0 == fragmentsRead)
        {
            Thread.yield();
            Tests.checkInterruptStatus();
        }

        return fragmentsRead;
    }
}

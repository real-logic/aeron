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
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.RawBlockHandler;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.Publication.CLOSED;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.asList;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(InterruptingTestCallback.class)
@SuppressWarnings("try")
class ExclusivePublicationTest
{
    private static List<String> channels()
    {
        return asList(
            "aeron:udp?endpoint=224.20.30.39:24323|interface=localhost|term-length=64k",
            "aeron:udp?endpoint=localhost:24325|term-length=64k",
            "aeron:ipc?term-length=64k");
    }

    private static final int STREAM_ID = 1007;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = 200;

    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[65 * 1024]);

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private final MediaDriver.Context driverContext = new MediaDriver.Context()
        .errorHandler(Tests::onError)
        .sharedIdleStrategy(YieldingIdleStrategy.INSTANCE)
        .threadingMode(ThreadingMode.SHARED);

    private TestMediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void setUp(final @TempDir Path tempDir)
    {
        driver = TestMediaDriver.launch(
            driverContext.aeronDirectoryName(tempDir.resolve("driver").toString()), testWatcher);
        testWatcher.dataCollector().add(driver.context().aeronDirectory());

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldPublishFromIndependentExclusivePublications(final String channel)
    {
        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publicationOne = aeron.addExclusivePublication(channel, STREAM_ID);
            ExclusivePublication publicationTwo = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            final int expectedNumberOfFragments = 778;
            int totalFragmentsRead = 0;
            final MutableInteger messageCount = new MutableInteger();
            final FragmentHandler fragmentHandler =
                (buffer, offset, length, header) ->
                {
                    assertEquals(MESSAGE_LENGTH, length);
                    messageCount.value++;
                };

            Tests.awaitConnections(subscription, 2);

            for (int i = 0; i < expectedNumberOfFragments; i += 2)
            {
                while (publicationOne.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
                {
                    Tests.yield();
                    totalFragmentsRead += pollFragments(subscription, fragmentHandler);
                }

                while (publicationTwo.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
                {
                    Tests.yield();
                    totalFragmentsRead += pollFragments(subscription, fragmentHandler);
                }

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

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldPublishFromConcurrentExclusivePublications(final String channel)
    {
        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publicationOne = aeron.addExclusivePublication(channel, STREAM_ID);
            ExclusivePublication publicationTwo = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            final int expectedNumberOfFragments = 20_000;
            final int fragmentsPerThread = expectedNumberOfFragments / 2;
            final MutableInteger messageCount = new MutableInteger();
            final FragmentHandler fragmentHandler =
                (buffer, offset, length, header) ->
                {
                    assertEquals(MESSAGE_LENGTH, length);
                    messageCount.value++;
                };

            Tests.awaitConnections(subscription, 2);

            final ExecutorService threadPool = Executors.newFixedThreadPool(2);
            try
            {
                final CountDownLatch latch = new CountDownLatch(2);
                threadPool.submit(
                    () ->
                    {
                        latch.countDown();
                        latch.await();
                        for (int count = 0; count < fragmentsPerThread; count++)
                        {
                            while (publicationOne.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
                            {
                                Tests.yield();
                            }
                        }
                        return null;
                    });
                threadPool.submit(
                    () ->
                    {
                        latch.countDown();
                        latch.await();
                        for (int count = 0; count < fragmentsPerThread; count++)
                        {
                            while (publicationTwo.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
                            {
                                Tests.yield();
                            }
                        }
                        return null;
                    });

                int totalFragmentsRead = 0;
                do
                {
                    totalFragmentsRead += pollFragments(subscription, fragmentHandler);
                }
                while (totalFragmentsRead < expectedNumberOfFragments);
            }
            finally
            {
                threadPool.shutdownNow();
            }

            assertEquals(expectedNumberOfFragments, messageCount.value);
        }
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldOfferTwoBuffersFromIndependentExclusivePublications(final String channel)
    {
        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publicationOne = aeron.addExclusivePublication(channel, STREAM_ID);
            ExclusivePublication publicationTwo = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            final int expectedNumberOfFragments = 778;
            int totalFragmentsRead = 0;
            final MutableInteger messageCount = new MutableInteger();
            final FragmentHandler fragmentHandler =
                (buffer, offset, length, header) ->
                {
                    assertEquals(MESSAGE_LENGTH + SIZE_OF_INT, length);
                    final int publisherId = buffer.getInt(offset);
                    if (1 == publisherId)
                    {
                        assertEquals(Byte.MIN_VALUE, buffer.getByte(offset + SIZE_OF_INT));
                    }
                    else if (2 == publisherId)
                    {
                        assertEquals(Byte.MAX_VALUE, buffer.getByte(offset + SIZE_OF_INT));
                    }
                    else
                    {
                        fail("unknown publisherId=" + publisherId);
                    }
                    messageCount.value++;
                };

            Tests.awaitConnections(subscription, 2);

            final UnsafeBuffer pubOneHeader = new UnsafeBuffer(new byte[SIZE_OF_INT]);
            pubOneHeader.putInt(0, 1);
            final UnsafeBuffer pubOnePayload = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
            pubOnePayload.setMemory(0, MESSAGE_LENGTH, Byte.MIN_VALUE);
            final UnsafeBuffer pubTwoHeader = new UnsafeBuffer(new byte[SIZE_OF_INT]);
            pubTwoHeader.putInt(0, 2);
            final UnsafeBuffer pubTwoPayload = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
            pubTwoPayload.setMemory(0, MESSAGE_LENGTH, Byte.MAX_VALUE);

            for (int i = 0; i < expectedNumberOfFragments; i += 2)
            {
                while (publicationOne.offer(pubOneHeader, 0, SIZE_OF_INT, pubOnePayload, 0, MESSAGE_LENGTH) < 0L)
                {
                    Tests.yield();
                    totalFragmentsRead += pollFragments(subscription, fragmentHandler);
                }

                while (publicationTwo.offer(pubTwoHeader, 0, SIZE_OF_INT, pubTwoPayload, 0, MESSAGE_LENGTH) < 0L)
                {
                    Tests.yield();
                    totalFragmentsRead += pollFragments(subscription, fragmentHandler);
                }

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

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldOfferTwoBuffersFromConcurrentExclusivePublications(final String channel)
    {
        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publicationOne = aeron.addExclusivePublication(channel, STREAM_ID);
            ExclusivePublication publicationTwo = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            final int expectedNumberOfFragments = 20_000;
            final int fragmentsPerThread = expectedNumberOfFragments / 2;
            final MutableInteger messageCount = new MutableInteger();
            final FragmentHandler fragmentHandler =
                (buffer, offset, length, header) ->
                {
                    assertEquals(MESSAGE_LENGTH + SIZE_OF_INT, length);
                    final int publisherId = buffer.getInt(offset);
                    if (1 == publisherId)
                    {
                        assertEquals(Byte.MIN_VALUE, buffer.getByte(offset + SIZE_OF_INT));
                    }
                    else if (2 == publisherId)
                    {
                        assertEquals(Byte.MAX_VALUE, buffer.getByte(offset + SIZE_OF_INT));
                    }
                    else
                    {
                        fail("unknown publisherId=" + publisherId);
                    }
                    messageCount.value++;
                };

            Tests.awaitConnections(subscription, 2);

            final UnsafeBuffer pubOneHeader = new UnsafeBuffer(new byte[SIZE_OF_INT]);
            pubOneHeader.putInt(0, 1);
            final UnsafeBuffer pubOnePayload = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
            pubOnePayload.setMemory(0, MESSAGE_LENGTH, Byte.MIN_VALUE);
            final UnsafeBuffer pubTwoHeader = new UnsafeBuffer(new byte[SIZE_OF_INT]);
            pubTwoHeader.putInt(0, 2);
            final UnsafeBuffer pubTwoPayload = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
            pubTwoPayload.setMemory(0, MESSAGE_LENGTH, Byte.MAX_VALUE);

            final ExecutorService threadPool = Executors.newFixedThreadPool(2);
            try
            {
                final CountDownLatch latch = new CountDownLatch(2);
                threadPool.submit(
                    () ->
                    {
                        latch.countDown();
                        latch.await();
                        for (int count = 0; count < fragmentsPerThread; count++)
                        {
                            while (publicationOne
                                .offer(pubOneHeader, 0, SIZE_OF_INT, pubOnePayload, 0, MESSAGE_LENGTH) < 0L)
                            {
                                Tests.yield();
                            }
                        }
                        return null;
                    });
                threadPool.submit(
                    () ->
                    {
                        latch.countDown();
                        latch.await();
                        for (int count = 0; count < fragmentsPerThread; count++)
                        {
                            while (publicationTwo
                                .offer(pubTwoHeader, 0, SIZE_OF_INT, pubTwoPayload, 0, MESSAGE_LENGTH) < 0L)
                            {
                                Tests.yield();
                            }
                        }
                        return null;
                    });
                threadPool.shutdown();

                int totalFragmentsRead = 0;
                do
                {
                    totalFragmentsRead += pollFragments(subscription, fragmentHandler);
                }
                while (totalFragmentsRead < expectedNumberOfFragments);
            }
            finally
            {
                threadPool.shutdownNow();
            }

            assertEquals(expectedNumberOfFragments, messageCount.value);
        }
    }

    @Test
    @InterruptAfter(10)
    void offerBlockThrowsIllegalArgumentExceptionIfLengthExceedsAvailableSpaceWithinTheTerm()
    {
        final String channel = "aeron:ipc?term-length=64k";
        try (Subscription ignore = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            while (publication.offer(srcBuffer, 0, MESSAGE_LENGTH) < 0L)
            {
                Tests.yield();
            }

            final int termBufferLength = publication.termBufferLength();
            final int termOffset = publication.termOffset();
            final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> publication.offerBlock(srcBuffer, 0, termBufferLength));

            assertEquals("invalid block length " + termBufferLength +
                ", remaining space in term is " + (termBufferLength - termOffset), exception.getMessage());
        }
    }

    @Test
    @InterruptAfter(10)
    void offerBlockReturnsClosedWhenPublicationIsClosed()
    {
        final String channel = "aeron:ipc?term-length=64k";
        try (Subscription ignore = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            publication.close();

            final long result = publication.offerBlock(srcBuffer, 0, MESSAGE_LENGTH);
            assertEquals(CLOSED, result);
        }
    }

    @Test
    @InterruptAfter(10)
    void offerBlockThrowsIllegalArgumentExceptionUponInvalidFirstFrame()
    {
        final String channel = "aeron:ipc?term-length=64k";
        try (Subscription ignore = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            final int sessionId = publication.sessionId();
            final int streamId = publication.streamId();
            final int termId = publication.termId();
            final int offset = 128;

            frameType(srcBuffer, offset, HDR_TYPE_NAK);
            frameSessionId(srcBuffer, offset, -19);
            srcBuffer.putInt(offset + STREAM_ID_FIELD_OFFSET, 42, LITTLE_ENDIAN);

            while (publication.availableWindow() <= 0)
            {
                Tests.yield();
            }

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
    @InterruptAfter(10)
    void offerBlockAcceptsUpToAnEntireTermOfData()
    {
        final String channel = "aeron:ipc?term-length=64k";
        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID))
        {
            final int sessionId = publication.sessionId();
            final int streamId = publication.streamId();
            final int currentTermId = publication.termId();
            final int termBufferLength = publication.termBufferLength();
            final int offset = 1024;

            final RawBlockHandler rawBlockHandler =
                (fileChannel, fileOffset, termBuffer, termOffset, length, pollSessionId, termId) ->
                {
                    assertEquals(HDR_TYPE_DATA, frameType(termBuffer, termOffset));
                    assertEquals(termBufferLength, frameLength(termBuffer, termOffset));
                    assertEquals(sessionId, frameSessionId(termBuffer, termOffset));
                    assertEquals(streamId, termBuffer.getInt(termOffset + STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN));
                };

            frameType(srcBuffer, offset, HDR_TYPE_DATA);
            frameLengthOrdered(srcBuffer, offset, termBufferLength);
            frameSessionId(srcBuffer, offset, sessionId);
            srcBuffer.putInt(offset + STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);
            srcBuffer.putInt(offset + TERM_ID_FIELD_OFFSET, currentTermId, LITTLE_ENDIAN);
            srcBuffer.setMemory(offset + DATA_OFFSET, termBufferLength - HEADER_LENGTH, (byte)13);

            Tests.awaitConnections(subscription, 1);
            while (publication.availableWindow() <= 0)
            {
                Tests.yield();
            }

            final long position = publication.position();
            final long result = publication.offerBlock(srcBuffer, offset, termBufferLength);
            assertEquals(position + termBufferLength, result);

            final long pollBytes = subscription.rawPoll(rawBlockHandler, termBufferLength);

            assertEquals(termBufferLength, pollBytes);
            assertEquals(publication.termBufferLength(), publication.termOffset());
            assertEquals(currentTermId, publication.termId());
        }
    }

    @Test
    @InterruptAfter(10)
    void offerBlockReturnsBackPressuredStatus()
    {
        final String channel = "aeron:ipc?term-length=64k";
        try (Subscription ignore = aeron.addSubscription(channel, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(channel, STREAM_ID))
        {
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

            while (publication.offerBlock(srcBuffer, offset, length) < 0)
            {
                Tests.yield();
            }

            final long result = publication.offerBlock(srcBuffer, 0, offset);
            assertEquals(BACK_PRESSURED, result);
        }
    }

    private int pollFragments(final Subscription subscription, final FragmentHandler fragmentHandler)
    {
        final int fragmentsRead = subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
        if (0 == fragmentsRead)
        {
            Tests.yield();
        }

        return fragmentsRead;
    }
}

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
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.InterruptAfter;
import io.aeron.test.RandomWatcher;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SpecifiedPositionPublicationTest
{
    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();
    @RegisterExtension
    final RandomWatcher randomWatcher = new RandomWatcher();

    @InterruptAfter(5)
    @ParameterizedTest
    @CsvSource({
        CommonContext.IPC_CHANNEL + ",true",
        "aeron:udp?endpoint=localhost:24325,true",
        CommonContext.IPC_CHANNEL + ",false",
        "aeron:udp?endpoint=localhost:24325,false"
    })
    void shouldStartAtSpecifiedPositionForPublications(final String initialUri, final boolean exclusive)
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .ipcPublicationTermWindowLength(LogBufferDescriptor.TERM_MIN_LENGTH)
            .threadingMode(ThreadingMode.SHARED);
        final DirectBuffer msg = new UnsafeBuffer(new byte[64]);

        final int termLength = 1 << 16;
        final int initialTermId = randomWatcher.random().nextInt();
        final int activeTermId = initialTermId + randomWatcher.random().nextInt(Integer.MAX_VALUE);
        final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
        final int termOffset = randomWatcher.random().nextInt(termLength) & -FrameDescriptor.FRAME_ALIGNMENT;
        final long startPosition = LogBufferDescriptor.computePosition(
            activeTermId,
            termOffset,
            positionBitsToShift,
            initialTermId);
        final PositionCalculator positionCalculator = new PositionCalculator(startPosition, termLength, termOffset);
        final long nextPosition = positionCalculator.addMessage(DataHeaderFlyweight.HEADER_LENGTH + msg.capacity());

        final String channel = new ChannelUriStringBuilder(initialUri)
            .initialPosition(startPosition, initialTermId, termLength)
            .build();

        final int streamId = 1001;
        final Function<Aeron, Publication> publicationSupplier = exclusive ?
            (a) -> a.addExclusivePublication(channel, streamId) : (a) -> a.addPublication(channel, streamId);

        try (
            TestMediaDriver mediaDriver = TestMediaDriver.launch(context, testWatcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            Subscription subscription = aeron.addSubscription(initialUri, streamId);
            Publication publication = publicationSupplier.apply(aeron))
        {
            Tests.awaitConnected(subscription);
            Tests.awaitConnected(publication);

            assertEquals(startPosition, publication.position());

            Tests.await(() -> publication.offer(msg) > 0);
            assertEquals(nextPosition, publication.position());

            final FragmentHandler fragmentHandler =
                (buffer, offset, length, header) -> assertEquals(nextPosition, header.position());

            Tests.await(() -> subscription.poll(fragmentHandler, 1) == 1);
        }
        finally
        {
            context.deleteDirectory();
        }
    }

    @InterruptAfter(5)
    @ParameterizedTest
    @CsvSource({
        CommonContext.IPC_CHANNEL,
        "aeron:udp?endpoint=localhost:24325"
    })
    void shouldValidateSpecifiedPositionForConcurrentPublications(final String initialUri)
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .ipcPublicationTermWindowLength(LogBufferDescriptor.TERM_MIN_LENGTH)
            .threadingMode(ThreadingMode.SHARED);
        final DirectBuffer msg = new UnsafeBuffer(new byte[64]);

        final int termLength = 1 << 16;
        final int initialTermId = randomWatcher.random().nextInt();
        final int activeTermId = initialTermId + randomWatcher.random().nextInt(Integer.MAX_VALUE);
        final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
        final int termOffset = randomWatcher.random().nextInt(termLength) & -FrameDescriptor.FRAME_ALIGNMENT;
        final long startPosition = LogBufferDescriptor.computePosition(
            activeTermId,
            termOffset,
            positionBitsToShift,
            initialTermId);
        final int totalMessageLength = DataHeaderFlyweight.HEADER_LENGTH + msg.capacity();
        final PositionCalculator positionCalculator = new PositionCalculator(startPosition, termLength, termOffset);
        final long positionMsg1 = positionCalculator.addMessage(totalMessageLength);
        final long positionMsg2 = positionCalculator.addMessage(totalMessageLength);
        final long positionMsg3 = positionCalculator.addMessage(totalMessageLength);

        final String channel = new ChannelUriStringBuilder(initialUri)
            .initialPosition(startPosition, initialTermId, termLength)
            .build();

        final String invalidPositionUri = new ChannelUriStringBuilder(initialUri)
            .initialPosition(startPosition + FrameDescriptor.FRAME_ALIGNMENT, initialTermId, termLength)
            .build();
        final String invalidInitialTermIdUri = new ChannelUriStringBuilder(initialUri)
            .initialPosition(startPosition, initialTermId + 1, termLength)
            .build();
        final String invalidTermLengthUri = new ChannelUriStringBuilder(initialUri)
            .initialPosition(startPosition, initialTermId, termLength << 1)
            .build();

        final int streamId = 1001;

        try (
            TestMediaDriver mediaDriver = TestMediaDriver.launch(context, testWatcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            Subscription subscription = aeron.addSubscription(initialUri, streamId);
            Publication publication = aeron.addPublication(channel, streamId))
        {
            Tests.awaitConnected(subscription);
            Tests.awaitConnected(publication);

            assertEquals(startPosition, publication.position());

            Tests.await(() -> publication.offer(msg) > 0);
            assertEquals(positionMsg1, publication.position());

            Tests.await(() -> subscription.poll((buffer, offset, length, header) -> {}, 1) == 1);

            try (Publication publication2 = aeron.addPublication(channel, streamId))
            {
                assertEquals(positionMsg1, publication2.position());
                Tests.await(() -> publication.offer(msg) > 0);
                assertEquals(positionMsg2, publication2.position());
                final FragmentHandler fragmentHandler =
                    (buffer, offset, length, header) -> assertEquals(positionMsg2, header.position());
                Tests.await(() -> subscription.poll(fragmentHandler, 1) == 1);
            }

            try (Publication publication3 = aeron.addPublication(initialUri, streamId))
            {
                assertEquals(positionMsg2, publication3.position());
                Tests.await(() -> publication.offer(msg) > 0);
                assertEquals(positionMsg3, publication3.position());
                final FragmentHandler fragmentHandler =
                    (buffer, offset, length, header) -> assertEquals(positionMsg3, header.position());
                Tests.await(() -> subscription.poll(fragmentHandler, 1) == 1);
            }

            assertThrows(RegistrationException.class, () -> aeron.addPublication(invalidPositionUri, streamId));
            assertThrows(RegistrationException.class, () -> aeron.addPublication(invalidInitialTermIdUri, streamId));
            assertThrows(RegistrationException.class, () -> aeron.addPublication(invalidTermLengthUri, streamId));
        }
        finally
        {
            context.deleteDirectory();
        }
    }

    @InterruptAfter(5)
    @ParameterizedTest
    @CsvSource({
        CommonContext.IPC_CHANNEL,
        "aeron:udp?endpoint=localhost:24325"
    })
    void shouldValidateSpecifiedPositionForConcurrentPublicationsInitiallyUnspecified(final String initialUri)
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .ipcPublicationTermWindowLength(LogBufferDescriptor.TERM_MIN_LENGTH)
            .threadingMode(ThreadingMode.SHARED);

        final int streamId = 1001;

        try (
            TestMediaDriver mediaDriver = TestMediaDriver.launch(context, testWatcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            Subscription subscription = aeron.addSubscription(initialUri, streamId);
            Publication publication = aeron.addPublication(initialUri, streamId))
        {
            Tests.awaitConnected(subscription);
            Tests.awaitConnected(publication);

            final String channel = new ChannelUriStringBuilder(initialUri)
                .initialPosition(publication.position(), publication.initialTermId(), publication.termBufferLength())
                .build();

            try (Publication publication2 = aeron.addPublication(channel, streamId))
            {
                assertEquals(publication.position(), publication2.position());
                assertEquals(publication.initialTermId(), publication2.initialTermId());
            }
        }
        finally
        {
            context.deleteDirectory();
        }
    }

    static final class PositionCalculator
    {
        final int termLength;
        long position;
        int termRemaining;

        PositionCalculator(final long startingPosition, final int termLength, final int termOffset)
        {
            this.position = startingPosition;
            this.termLength = termLength;
            this.termRemaining = termLength - termOffset;
        }

        long addMessage(final int totalMessageLength)
        {
            if (termRemaining < totalMessageLength)
            {
                position += termRemaining;
                termRemaining = termLength;
            }

            position += totalMessageLength;
            termRemaining -= totalMessageLength;

            return position;
        }
    }
}

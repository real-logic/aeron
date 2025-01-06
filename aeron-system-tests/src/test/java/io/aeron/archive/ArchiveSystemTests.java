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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.Tests;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ArchiveSystemTests
{
    static final long CATALOG_CAPACITY = 128 * 1024;
    static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    static final int FRAGMENT_LIMIT = 10;

    static void offer(final Publication publication, final int count, final String prefix)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        for (int i = 0; i < count; i++)
        {
            final int length = buffer.putStringWithoutLengthAscii(0, prefix + i);

            while (publication.offer(buffer, 0, length) <= 0)
            {
                Tests.yield();
            }
        }
    }

    static void offerToPosition(final Publication publication, final String prefix, final long minimumPosition)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        for (int i = 0; publication.position() < minimumPosition; i++)
        {
            final int length = buffer.putStringWithoutLengthAscii(0, prefix + i);

            while (publication.offer(buffer, 0, length) <= 0)
            {
                Tests.yield();
            }
        }
    }

    static void consume(final Subscription subscription, final int count, final String prefix)
    {
        final MutableInteger received = new MutableInteger(0);

        final FragmentHandler fragmentHandler = new FragmentAssembler(
            (buffer, offset, length, header) ->
            {
                final String expected = prefix + received.value;
                final String actual = buffer.getStringWithoutLengthAscii(offset, length);

                assertEquals(expected, actual);

                received.value++;
            });

        while (received.value < count)
        {
            if (0 == subscription.poll(fragmentHandler, FRAGMENT_LIMIT))
            {
                Tests.yield();
            }
        }

        assertEquals(count, received.get());
    }

    static TestRecordingSignalConsumer injectRecordingSignalConsumer(final AeronArchive aeronArchive)
    {
        final long controlSessionId = aeronArchive.controlSessionId();
        final TestRecordingSignalConsumer recordingSignalConsumer = new TestRecordingSignalConsumer(controlSessionId);
        aeronArchive.context().recordingSignalConsumer(recordingSignalConsumer);
        return recordingSignalConsumer;
    }

    static void awaitSignal(
        final AeronArchive aeronArchive,
        final TestRecordingSignalConsumer signalConsumer,
        final RecordingSignal expectedSignal)
    {
        while (expectedSignal != signalConsumer.signal)
        {
            if (0 == aeronArchive.pollForRecordingSignals())
            {
                Tests.yield();
            }
        }
    }

    static void resetAndAwaitSignal(
        final AeronArchive aeronArchive,
        final TestRecordingSignalConsumer signalConsumer,
        final RecordingSignal expectedSignal)
    {
        signalConsumer.reset();
        awaitSignal(aeronArchive, signalConsumer, expectedSignal);
    }

    static void awaitSignal(
        final AeronArchive aeronArchive,
        final TestRecordingSignalConsumer signalConsumer,
        final long expectedRecordingId,
        final RecordingSignal expectedSignal)
    {
        final Supplier<String> errorMessage = () -> "Expected signal: " + expectedSignal;
        while (expectedRecordingId != signalConsumer.recordingId || expectedSignal != signalConsumer.signal)
        {
            if (0 == aeronArchive.pollForRecordingSignals())
            {
                Tests.yieldingIdle(errorMessage);
            }
        }
    }

    static void resetAndAwaitSignal(
        final AeronArchive aeronArchive,
        final TestRecordingSignalConsumer signalConsumer,
        final long expectedRecordingId,
        final RecordingSignal expectedSignal)
    {
        signalConsumer.reset();
        awaitSignal(aeronArchive, signalConsumer, expectedRecordingId, expectedSignal);
    }

    static RecordingResult recordData(final AeronArchive aeronArchive)
    {
        final TestRecordingSignalConsumer testRecordingSignalConsumer = new TestRecordingSignalConsumer(
            aeronArchive.controlSessionId());
        aeronArchive.context().recordingSignalConsumer(testRecordingSignalConsumer);

        final UnsafeBuffer message = new UnsafeBuffer(new byte[1024]);
        message.setMemory(0, message.capacity(), (byte)'x');

        long recordingId;
        final long position;
        long halfWayPosition = Aeron.NULL_VALUE;
        try (Publication publication = aeronArchive.addRecordedPublication("aeron:ipc", 10000))
        {
            int messageCount = 1000;
            while (messageCount > 0)
            {
                if (0 < publication.offer(message))
                {
                    --messageCount;
                    if (Aeron.NULL_VALUE == halfWayPosition && messageCount == messageCount / 2)
                    {
                        halfWayPosition = publication.position();
                    }
                }
                else
                {
                    Tests.yield();
                }
            }

            position = publication.position();
            assertNotEquals(0, position);

            while (-1 == (recordingId = aeronArchive.findLastMatchingRecording(
                0, "aeron:ipc", publication.streamId(), publication.sessionId())))
            {
                Tests.yield();
            }

            while (aeronArchive.getRecordingPosition(recordingId) < position)
            {
                Tests.yield();
            }
        }

        while (testRecordingSignalConsumer.signal != RecordingSignal.STOP)
        {
            aeronArchive.pollForRecordingSignals();
            Tests.yield();
        }

        return new RecordingResult(position, halfWayPosition, recordingId);
    }

    static class RecordingResult
    {
        public final long position;
        public final long halfwayPosition;
        public final long recordingId;

        RecordingResult(final long position, final long halfwayPosition, final long recordingId)
        {
            this.position = position;
            this.halfwayPosition = halfwayPosition;
            this.recordingId = recordingId;
        }
    }
}

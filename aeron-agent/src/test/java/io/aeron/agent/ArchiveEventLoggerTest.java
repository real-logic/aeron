/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.agent;

import io.aeron.archive.codecs.ListRecordingRequestDecoder;
import io.aeron.archive.codecs.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.temporal.ChronoUnit;

import static io.aeron.agent.AgentTests.verifyLogHeader;
import static io.aeron.agent.ArchiveEventCode.*;
import static io.aeron.agent.ArchiveEventEncoder.replicationSessionDoneLength;
import static io.aeron.agent.ArchiveEventLogger.CONTROL_REQUEST_EVENTS;
import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static io.aeron.archive.codecs.MessageHeaderEncoder.ENCODED_LENGTH;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.*;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.*;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.junit.jupiter.params.provider.EnumSource.Mode.INCLUDE;

class ArchiveEventLoggerTest
{
    private static final int CAPACITY = align(MAX_EVENT_LENGTH, CACHE_LINE_LENGTH) * 8;
    private final UnsafeBuffer logBuffer = new UnsafeBuffer(allocateDirect(CAPACITY + TRAILER_LENGTH));
    private final ArchiveEventLogger logger = new ArchiveEventLogger(new ManyToOneRingBuffer(logBuffer));
    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH * 3]);

    @AfterEach
    void after()
    {
        ArchiveComponentLogger.ENABLED_EVENTS.clear();
        EventConfiguration.EVENT_RING_BUFFER.unblock();
    }

    @ParameterizedTest
    @EnumSource(
        value = ArchiveEventCode.class,
        mode = EXCLUDE,
        names = {
            "CMD_OUT_RESPONSE", "REPLICATION_SESSION_STATE_CHANGE",
            "CONTROL_SESSION_STATE_CHANGE", "REPLAY_SESSION_ERROR", "CATALOG_RESIZE",
            "REPLICATION_SESSION_DONE", "REPLAY_SESSION_STATE_CHANGE", "RECORDING_SESSION_STATE_CHANGE"
        })
    void logControlRequest(final ArchiveEventCode eventCode)
    {
        ArchiveComponentLogger.ENABLED_EVENTS.add(eventCode);
        final int srcOffset = 100;
        final int length = MAX_EVENT_LENGTH * 2;
        new MessageHeaderEncoder().wrap(srcBuffer, srcOffset).templateId(eventCode.templateId());
        srcBuffer.setMemory(srcOffset + ENCODED_LENGTH, length, (byte)3);
        final int captureLength = MAX_CAPTURE_LENGTH;
        logBuffer.putLong(CAPACITY + HEAD_CACHE_POSITION_OFFSET, CAPACITY * 3L);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, 128 + CAPACITY * 3L);
        final int recordOffset = 128;

        logger.logControlRequest(srcBuffer, srcOffset, length);

        verifyLogHeader(logBuffer, recordOffset, eventCode.toEventCodeId(), captureLength, length);
        for (int i = 0; i < captureLength - ENCODED_LENGTH; i++)
        {
            assertEquals((byte)3,
                logBuffer.getByte(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + ENCODED_LENGTH + i)));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, ListRecordingRequestDecoder.TEMPLATE_ID })
    void logControlRequestNoOp(final int templateId)
    {
        final int srcOffset = 0;
        new MessageHeaderEncoder().wrap(srcBuffer, srcOffset).templateId(templateId);
        final int length = 100;
        srcBuffer.setMemory(srcOffset + ENCODED_LENGTH, length, (byte)3);
        final int recordOffset = 0;

        logger.logControlRequest(srcBuffer, srcOffset, length);

        assertEquals(0, logBuffer.getInt(lengthOffset(recordOffset), LITTLE_ENDIAN));
    }

    @Test
    void logControlResponse()
    {
        final int offset = 4;
        final int length = 64;
        srcBuffer.setMemory(0, offset, (byte)255);
        srcBuffer.setMemory(offset, length, (byte)1);
        final int recordOffset = HEADER_LENGTH * 5;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);

        logger.logControlResponse(srcBuffer, offset, length);

        verifyLogHeader(logBuffer, recordOffset, CMD_OUT_RESPONSE.toEventCodeId(), length, length);
        for (int i = 0; i < length; i++)
        {
            assertEquals((byte)1, logBuffer.getByte(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + i)));
        }
    }

    @Test
    void logRecordingSignal()
    {
        final int offset = 10;
        final int length = 31;
        srcBuffer.setMemory(0, offset, (byte)255);
        srcBuffer.setMemory(offset, length, (byte)3);
        final int recordOffset = HEADER_LENGTH * 7;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);

        logger.logRecordingSignal(srcBuffer, offset, length);

        verifyLogHeader(logBuffer, recordOffset, RECORDING_SIGNAL.toEventCodeId(), length, length);
        for (int i = 0; i < length; i++)
        {
            assertEquals((byte)3, logBuffer.getByte(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + i)));
        }
    }

    @ParameterizedTest
    @EnumSource(
        value = ArchiveEventCode.class,
        mode = EXCLUDE,
        names = { "CMD_OUT_RESPONSE", "REPLICATION_SESSION_STATE_CHANGE",
            "REPLAY_SESSION_STATE_CHANGE", "RECORDING_SESSION_STATE_CHANGE",
            "CONTROL_SESSION_STATE_CHANGE", "REPLAY_SESSION_ERROR", "CATALOG_RESIZE" })
    void controlRequestEvents(final ArchiveEventCode eventCode)
    {
        assertTrue(CONTROL_REQUEST_EVENTS.contains(eventCode));
    }

    @ParameterizedTest
    @EnumSource(
        value = ArchiveEventCode.class,
        mode = INCLUDE,
        names = { "CMD_OUT_RESPONSE", "REPLICATION_SESSION_STATE_CHANGE",
            "REPLAY_SESSION_STATE_CHANGE", "RECORDING_SESSION_STATE_CHANGE",
            "CONTROL_SESSION_STATE_CHANGE", "REPLAY_SESSION_ERROR", "CATALOG_RESIZE" })
    void nonControlRequestEvents(final ArchiveEventCode eventCode)
    {
        assertFalse(CONTROL_REQUEST_EVENTS.contains(eventCode));
    }

    @Test
    void logSessionStateChange()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final ChronoUnit from = ChronoUnit.CENTURIES;
        final ChronoUnit to = ChronoUnit.MICROS;
        final long id = 555_000_000_000L;
        final long position = 827342L;
        final String payload = from.name() + STATE_SEPARATOR + to.name();
        final int captureLength = 2 * SIZE_OF_LONG + SIZE_OF_INT + payload.length();

        logger.logSessionStateChange(CONTROL_SESSION_STATE_CHANGE, from, to, id, position);

        verifyLogHeader(
            logBuffer, offset, CONTROL_SESSION_STATE_CHANGE.toEventCodeId(), captureLength, captureLength);
        assertEquals(id, logBuffer.getLong(encodedMsgOffset(offset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(
            position, logBuffer.getLong(encodedMsgOffset(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(
            payload, logBuffer.getStringAscii(encodedMsgOffset(offset + LOG_HEADER_LENGTH + 2 * SIZE_OF_LONG)));
    }

    @Test
    void logReplaySessionError()
    {
        final int offset = ALIGNMENT * 5 + 128;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long sessionId = 123;
        final long recordingId = Long.MIN_VALUE;
        final String errorMessage = "the error";
        final int captureLength = SIZE_OF_LONG * 2 + SIZE_OF_INT + errorMessage.length();

        logger.logReplaySessionError(sessionId, recordingId, errorMessage);

        verifyLogHeader(logBuffer, offset, REPLAY_SESSION_ERROR.toEventCodeId(), captureLength, captureLength);
        assertEquals(sessionId, logBuffer.getLong(encodedMsgOffset(offset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(recordingId,
            logBuffer.getLong(encodedMsgOffset(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(errorMessage,
            logBuffer.getStringAscii(encodedMsgOffset(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG * 2)));
    }

    @Test
    void logCatalogResize()
    {
        final int offset = ALIGNMENT * 3;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final int captureLength = SIZE_OF_LONG * 2;
        final long catalogLength = 42;
        final long newCatalogLength = 142;

        logger.logCatalogResize(catalogLength, newCatalogLength);

        verifyLogHeader(logBuffer, offset, CATALOG_RESIZE.toEventCodeId(), captureLength, captureLength);
        assertEquals(catalogLength,
            logBuffer.getLong(encodedMsgOffset(offset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(newCatalogLength,
            logBuffer.getLong(encodedMsgOffset(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG), LITTLE_ENDIAN));
    }

    @Test
    void logReplicationSessionDone()
    {
        final long controlSessionId = 232345;
        final long replicationId = 456456;
        final long srcRecordingId = 345123;
        final long replayPosition = 2345;
        final long srcStopPosition = 3245;
        final long dstRecordingId = 435675346;
        final long dstStopPosition = 5685623;
        final long position = 3425234;
        final boolean isClosed = true;
        final boolean isEndOfStream = true;
        final boolean isSynced = false;

        final int offset = ALIGNMENT * 3;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);

        logger.logReplicationSessionDone(
            REPLICATION_SESSION_DONE,
            controlSessionId,
            replicationId,
            srcRecordingId,
            replayPosition,
            srcStopPosition,
            dstRecordingId,
            dstStopPosition,
            position,
            isClosed,
            isEndOfStream,
            isSynced);

        verifyLogHeader(
            logBuffer,
            offset,
            REPLICATION_SESSION_DONE.toEventCodeId(),
            replicationSessionDoneLength(),
            replicationSessionDoneLength());

        final StringBuilder sb = new StringBuilder();
        ArchiveEventDissector.dissectReplicationSessionDone(
            logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern =
            "\\[[0-9]+\\.[0-9]+] ARCHIVE: REPLICATION_SESSION_DONE \\[67/67]: controlSessionId=" + controlSessionId +
            " replicationId=" + replicationId + " srcRecordingId=" + srcRecordingId +
            " replayPosition=" + replayPosition + " srcStopPosition=" + srcStopPosition +
            " dstRecordingId=" + dstRecordingId + " dstStopPosition=" + dstStopPosition + " position=" + position +
            " isClosed=" + isClosed + " isEndOfStream=" + isEndOfStream + " isSynced=" + isSynced;

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }
}

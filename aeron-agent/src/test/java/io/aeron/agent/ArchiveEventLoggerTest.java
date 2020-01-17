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
package io.aeron.agent;

import io.aeron.archive.codecs.ListRecordingRequestDecoder;
import io.aeron.archive.codecs.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.agent.ArchiveEventCode.CMD_OUT_RESPONSE;
import static io.aeron.agent.ArchiveEventCode.EVENT_CODE_TYPE;
import static io.aeron.agent.ArchiveEventLogger.CONTROL_REQUEST_EVENTS;
import static io.aeron.agent.ArchiveEventLogger.toEventCodeId;
import static io.aeron.agent.Common.verifyLogHeader;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.CommonEventEncoder.MAX_CAPTURE_LENGTH;
import static io.aeron.agent.EventConfiguration.*;
import static io.aeron.archive.codecs.MessageHeaderEncoder.ENCODED_LENGTH;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.encodedMsgOffset;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.lengthOffset;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.junit.jupiter.params.provider.EnumSource.Mode.INCLUDE;

class ArchiveEventLoggerTest
{
    private final UnsafeBuffer logBuffer = new UnsafeBuffer(allocateDirect(MAX_EVENT_LENGTH * 8 + TRAILER_LENGTH));
    private final ArchiveEventLogger logger = new ArchiveEventLogger(new ManyToOneRingBuffer(logBuffer));
    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirect(MAX_EVENT_LENGTH * 3));

    @AfterEach
    void after()
    {
        reset();
    }

    @ParameterizedTest
    @EnumSource(ArchiveEventCode.class)
    void toEventCodeIdComputesEventId(final ArchiveEventCode eventCode)
    {
        assertEquals(0xFFFF + EVENT_CODE_TYPE + eventCode.id(), toEventCodeId(eventCode));
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveEventCode.class, mode = EXCLUDE, names = { "CMD_OUT_RESPONSE" })
    void logControlRequest(final ArchiveEventCode eventCode)
    {
        ARCHIVE_EVENT_CODES.add(eventCode);
        final int offset = 100;
        final int dataLength = MAX_EVENT_LENGTH * 2;
        new MessageHeaderEncoder().wrap(buffer, offset).templateId(eventCode.templateId());
        buffer.setMemory(offset + ENCODED_LENGTH, dataLength, (byte)3);
        final int captureLength = MAX_CAPTURE_LENGTH;

        logger.logControlRequest(buffer, offset, dataLength);

        verifyLogHeader(logBuffer, toEventCodeId(eventCode), captureLength, captureLength, MAX_EVENT_LENGTH * 2);
        for (int i = 0; i < captureLength - ENCODED_LENGTH; i++)
        {
            assertEquals((byte)3, logBuffer.getByte(encodedMsgOffset(LOG_HEADER_LENGTH + ENCODED_LENGTH + i)));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, ListRecordingRequestDecoder.TEMPLATE_ID })
    void logControlRequestNoOp(final int templateId)
    {
        new MessageHeaderEncoder().wrap(buffer, 0).templateId(templateId);
        buffer.setMemory(ENCODED_LENGTH, 100, (byte)3);

        logger.logControlRequest(buffer, 0, 100);

        assertEquals(0, logBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN));
    }

    @Test
    void logControlResponse()
    {
        buffer.setMemory(0, 64, (byte)1);

        logger.logControlResponse(buffer, 64);

        verifyLogHeader(logBuffer, toEventCodeId(CMD_OUT_RESPONSE), 64, 64, 64);
        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte)1, logBuffer.getByte(encodedMsgOffset(LOG_HEADER_LENGTH + i)));
        }
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveEventCode.class, mode = EXCLUDE, names = { "CMD_OUT_RESPONSE" })
    void controlRequestEvents(final ArchiveEventCode eventCode)
    {
        assertTrue(CONTROL_REQUEST_EVENTS.contains(eventCode));
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveEventCode.class, mode = INCLUDE, names = { "CMD_OUT_RESPONSE" })
    void nonControlRequestEvents(final ArchiveEventCode eventCode)
    {
        assertFalse(CONTROL_REQUEST_EVENTS.contains(eventCode));
    }
}

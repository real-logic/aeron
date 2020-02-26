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
import static io.aeron.agent.AgentTests.verifyLogHeader;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.CommonEventEncoder.MAX_CAPTURE_LENGTH;
import static io.aeron.agent.EventConfiguration.*;
import static io.aeron.archive.codecs.MessageHeaderEncoder.ENCODED_LENGTH;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BitUtil.align;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.*;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.junit.jupiter.params.provider.EnumSource.Mode.INCLUDE;

class ArchiveEventLoggerTest
{
    private static final int CAPACITY = align(MAX_EVENT_LENGTH, CACHE_LINE_LENGTH) * 8;
    private final UnsafeBuffer logBuffer = new UnsafeBuffer(allocate(CAPACITY + TRAILER_LENGTH));
    private final ArchiveEventLogger logger = new ArchiveEventLogger(new ManyToOneRingBuffer(logBuffer));
    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH * 3]);

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
        final int srcOffset = 100;
        final int length = MAX_EVENT_LENGTH * 2;
        new MessageHeaderEncoder().wrap(srcBuffer, srcOffset).templateId(eventCode.templateId());
        srcBuffer.setMemory(srcOffset + ENCODED_LENGTH, length, (byte)3);
        final int captureLength = MAX_CAPTURE_LENGTH;
        logBuffer.putLong(CAPACITY + HEAD_CACHE_POSITION_OFFSET, CAPACITY * 3);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, 128 + CAPACITY * 3);
        final int recordOffset = 128;

        logger.logControlRequest(srcBuffer, srcOffset, length);

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(eventCode), captureLength, length);
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
        final int length = 64;
        srcBuffer.setMemory(0, length, (byte)1);
        final int recordOffset = HEADER_LENGTH * 5;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);

        logger.logControlResponse(srcBuffer, length);

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(CMD_OUT_RESPONSE), length, length);
        for (int i = 0; i < length; i++)
        {
            assertEquals((byte)1, logBuffer.getByte(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + i)));
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

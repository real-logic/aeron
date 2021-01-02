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
package io.aeron.agent;

import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.aeron.agent.AgentTests.verifyLogHeader;
import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverEventEncoder.untetheredSubscriptionStateChangeLength;
import static io.aeron.agent.DriverEventLogger.toEventCodeId;
import static io.aeron.agent.EventConfiguration.*;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.fill;
import static org.agrona.BitUtil.*;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.*;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TAIL_POSITION_OFFSET;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DriverEventLoggerTest
{
    private static final int CAPACITY = 32 * 1024;
    private final UnsafeBuffer logBuffer = new UnsafeBuffer(allocate(CAPACITY + TRAILER_LENGTH));
    private final DriverEventLogger logger = new DriverEventLogger(new ManyToOneRingBuffer(logBuffer));
    private final UnsafeBuffer buffer = new UnsafeBuffer(allocate(MAX_EVENT_LENGTH * 3));

    @AfterEach
    void after()
    {
        reset();
    }

    @ParameterizedTest
    @EnumSource(DriverEventCode.class)
    void toEventCodeIdComputesEventId(final DriverEventCode eventCode)
    {
        assertEquals(eventCode.id(), toEventCodeId(eventCode));
    }

    @Test
    void logIsNoOpIfEventIsNotEnabled()
    {
        buffer.setMemory(20, 100, (byte)5);

        logger.log(CMD_OUT_ERROR, buffer, 20, 100);

        assertEquals(0, logBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN));
    }

    @Test
    void log()
    {
        final DriverEventCode eventCode = CMD_IN_TERMINATE_DRIVER;
        DRIVER_EVENT_CODES.add(eventCode);
        final int recordOffset = align(13, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final int length = 100;
        final int srcOffset = 20;
        buffer.setMemory(srcOffset, length, (byte)5);

        logger.log(eventCode, buffer, srcOffset, length);

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(eventCode), length, length);
        for (int i = 0; i < length; i++)
        {
            assertEquals(5, logBuffer.getByte(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + i)));
        }
    }

    @Test
    void logFrameIn()
    {
        final int recordOffset = align(100, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final int length = 10_000;
        final int captureLength = MAX_CAPTURE_LENGTH;
        final int srcOffset = 4;
        buffer.setMemory(srcOffset, MAX_CAPTURE_LENGTH, (byte)3);
        final int encodedSocketLength = 12;

        logger.logFrameIn(buffer, srcOffset, length, new InetSocketAddress("localhost", 5555));

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(FRAME_IN), captureLength, length + encodedSocketLength);
        assertEquals(5555, logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(srcOffset,
            logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));

        for (int i = 0; i < captureLength - encodedSocketLength; i++)
        {
            assertEquals(3,
                logBuffer.getByte(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + encodedSocketLength + i)));
        }
    }

    @Test
    void logFrameOut()
    {
        final int recordOffset = 24;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final ByteBuffer byteBuffer = buffer.byteBuffer();
        byteBuffer.position(8);
        final byte[] bytes = new byte[32];
        fill(bytes, (byte)-1);
        byteBuffer.put(bytes);
        byteBuffer.flip().position(10).limit(38);
        final int encodedSocketLength = 12;
        final int length = byteBuffer.remaining() + encodedSocketLength;
        final int arrayCaptureLength = length - encodedSocketLength;

        logger.logFrameOut(byteBuffer, new InetSocketAddress("localhost", 3232));

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(FRAME_OUT), length, length);
        assertEquals(3232, logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(4,
            logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));

        for (int i = 0; i < arrayCaptureLength; i++)
        {
            assertEquals(-1,
                logBuffer.getByte(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + encodedSocketLength + i)));
        }
    }

    @Test
    void logString()
    {
        final int recordOffset = align(100, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final DriverEventCode eventCode = CMD_IN_ADD_PUBLICATION;
        final String value = "abc";
        final int captureLength = value.length() + SIZE_OF_INT;

        logger.logString(eventCode, value);

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(eventCode), captureLength, captureLength);
        assertEquals(value,
            logBuffer.getStringAscii(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
    }

    @Test
    void logPublicationRemoval()
    {
        final int recordOffset = align(1111, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final String uri = "uri";
        final int sessionId = 42;
        final int streamId = 19;
        final int captureLength = uri.length() + SIZE_OF_INT * 3;

        logger.logPublicationRemoval(uri, sessionId, streamId);

        verifyLogHeader(
            logBuffer, recordOffset, toEventCodeId(REMOVE_PUBLICATION_CLEANUP), captureLength, captureLength);
        assertEquals(sessionId, logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(streamId,
            logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
        assertEquals(uri,
            logBuffer.getStringAscii(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2),
            LITTLE_ENDIAN));
    }

    @Test
    void logSubscriptionRemoval()
    {
        final int recordOffset = align(131, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final String uri = "uri";
        final int streamId = 42;
        final long id = 19;
        final int captureLength = uri.length() + SIZE_OF_INT * 2 + SIZE_OF_LONG;

        logger.logSubscriptionRemoval(uri, streamId, id);

        verifyLogHeader(
            logBuffer, recordOffset, toEventCodeId(REMOVE_SUBSCRIPTION_CLEANUP), captureLength, captureLength);
        assertEquals(streamId, logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(id,
            logBuffer.getLong(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
        assertEquals(uri,
            logBuffer.getStringAscii(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT + SIZE_OF_LONG),
            LITTLE_ENDIAN));
    }

    @Test
    void logImageRemoval()
    {
        final int recordOffset = align(192, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final String uri = "uri";
        final int sessionId = 8;
        final int streamId = 61;
        final long id = 19;
        final int captureLength = uri.length() + SIZE_OF_INT * 3 + SIZE_OF_LONG;

        logger.logImageRemoval(uri, sessionId, streamId, id);

        verifyLogHeader(
            logBuffer, recordOffset, toEventCodeId(REMOVE_IMAGE_CLEANUP), captureLength, captureLength);
        assertEquals(sessionId, logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(streamId,
            logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
        assertEquals(id,
            logBuffer.getLong(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2), LITTLE_ENDIAN));
        assertEquals(uri, logBuffer.getStringAscii(
            encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2 + SIZE_OF_LONG), LITTLE_ENDIAN));
    }

    @Test
    void logUntetheredSubscriptionStateChange()
    {
        final int recordOffset = align(192, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final TimeUnit from = TimeUnit.DAYS;
        final TimeUnit to = TimeUnit.NANOSECONDS;
        final long subscriptionId = Long.MIN_VALUE;
        final int streamId = 61;
        final int sessionId = 8;
        final int captureLength = captureLength(untetheredSubscriptionStateChangeLength(from, to));

        logger.logUntetheredSubscriptionStateChange(from, to, subscriptionId, streamId, sessionId);

        verifyLogHeader(
            logBuffer, recordOffset, toEventCodeId(UNTETHERED_SUBSCRIPTION_STATE_CHANGE), captureLength, captureLength);
        assertEquals(subscriptionId,
            logBuffer.getLong(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(streamId,
            logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(sessionId,
            logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_LONG + SIZE_OF_INT),
            LITTLE_ENDIAN));
        assertEquals(from.name() + STATE_SEPARATOR + to.name(), logBuffer.getStringAscii(
            encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2 + SIZE_OF_LONG), LITTLE_ENDIAN));
    }

    @Test
    void logAddress()
    {
        final int recordOffset = 64;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, recordOffset);
        final DriverEventCode eventCode = NAME_RESOLUTION_NEIGHBOR_REMOVED;
        final int captureLength = 12;

        logger.logAddress(eventCode, new InetSocketAddress("localhost", 5656));

        verifyLogHeader(logBuffer, recordOffset, toEventCodeId(eventCode), captureLength, captureLength);
        assertEquals(5656, logBuffer.getInt(encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(4, logBuffer.getInt(
            encodedMsgOffset(recordOffset + LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
    }
}

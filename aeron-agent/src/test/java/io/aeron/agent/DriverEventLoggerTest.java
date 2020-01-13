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

import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.agent.Common.verifyLogHeader;
import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverEventLogger.toEventCodeId;
import static io.aeron.agent.EventConfiguration.*;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.fill;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.encodedMsgOffset;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.lengthOffset;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DriverEventLoggerTest
{
    private final UnsafeBuffer logBuffer = new UnsafeBuffer(allocateDirect(MAX_EVENT_LENGTH * 8 + TRAILER_LENGTH));
    private final DriverEventLogger logger = new DriverEventLogger(new ManyToOneRingBuffer(logBuffer));
    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirect(MAX_EVENT_LENGTH * 3));

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
        buffer.setMemory(20, 100, (byte)5);

        logger.log(eventCode, buffer, 20, 100);

        verifyLogHeader(logBuffer, toEventCodeId(eventCode), 100, 100, 100);
        for (int i = 0; i < 100; i++)
        {
            assertEquals(5, logBuffer.getByte(encodedMsgOffset(LOG_HEADER_LENGTH + i)));
        }
    }

    @Test
    void logFrameIn()
    {
        final int captureLength = MAX_CAPTURE_LENGTH - SOCKET_ADDRESS_MAX_LENGTH;
        buffer.setMemory(4, MAX_CAPTURE_LENGTH, (byte)3);

        logger.logFrameIn(buffer, 4, 10_000, new InetSocketAddress("localhost", 5555));

        verifyLogHeader(logBuffer, toEventCodeId(FRAME_IN), captureLength + 12, captureLength, 10_000);
        assertEquals(5555, logBuffer.getInt(encodedMsgOffset(LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(4, logBuffer.getInt(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
        for (int i = 0; i < captureLength; i++)
        {
            assertEquals(3, logBuffer.getByte(encodedMsgOffset(LOG_HEADER_LENGTH + 12 + i)));
        }
    }

    @Test
    void logFrameOut()
    {
        final ByteBuffer byteBuffer = this.buffer.byteBuffer();
        byteBuffer.position(8);
        final byte[] bytes = new byte[32];
        fill(bytes, (byte)-1);
        byteBuffer.put(bytes);
        byteBuffer.flip().position(10).limit(38);
        final int captureLength = 28;

        logger.logFrameOut(byteBuffer, new InetSocketAddress("localhost", 3232));

        verifyLogHeader(logBuffer, toEventCodeId(FRAME_OUT), captureLength + 12, captureLength, captureLength);
        assertEquals(3232, logBuffer.getInt(encodedMsgOffset(LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(4, logBuffer.getInt(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
        for (int i = 0; i < captureLength; i++)
        {
            assertEquals(-1, logBuffer.getByte(encodedMsgOffset(LOG_HEADER_LENGTH + 12 + i)));
        }
    }

    @Test
    void logString()
    {
        final DriverEventCode eventCode = CMD_IN_ADD_PUBLICATION;
        final String value = "abc";
        final int captureLength = value.length() + SIZE_OF_INT;

        logger.logString(eventCode, value);

        verifyLogHeader(logBuffer, toEventCodeId(eventCode), captureLength, captureLength, captureLength);
        assertEquals(value, logBuffer.getStringAscii(encodedMsgOffset(LOG_HEADER_LENGTH), LITTLE_ENDIAN));
    }

    @Test
    void logPublicationRemoval()
    {
        final String uri = "uri";
        final int sessionId = 42;
        final int streamId = 19;
        final int captureLength = uri.length() + SIZE_OF_INT * 3;

        logger.logPublicationRemoval(uri, sessionId, streamId);

        verifyLogHeader(
            logBuffer, toEventCodeId(REMOVE_PUBLICATION_CLEANUP), captureLength, captureLength, captureLength);
        assertEquals(sessionId, logBuffer.getInt(encodedMsgOffset(LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(streamId, logBuffer.getInt(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
        assertEquals(uri,
            logBuffer.getStringAscii(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_INT * 2), LITTLE_ENDIAN));
    }

    @Test
    void logSubscriptionRemoval()
    {
        final String uri = "uri";
        final int streamId = 42;
        final long id = 19;
        final int captureLength = uri.length() + SIZE_OF_INT * 2 + SIZE_OF_LONG;

        logger.logSubscriptionRemoval(uri, streamId, id);

        verifyLogHeader(
            logBuffer, toEventCodeId(REMOVE_SUBSCRIPTION_CLEANUP), captureLength, captureLength, captureLength);
        assertEquals(streamId, logBuffer.getInt(encodedMsgOffset(LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(id, logBuffer.getLong(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
        assertEquals(uri,
            logBuffer.getStringAscii(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_INT + SIZE_OF_LONG), LITTLE_ENDIAN));
    }

    @Test
    void logImageRemoval()
    {
        final String uri = "uri";
        final int sessionId = 8;
        final int streamId = 61;
        final long id = 19;
        final int captureLength = uri.length() + SIZE_OF_INT * 3 + SIZE_OF_LONG;

        logger.logImageRemoval(uri, sessionId, streamId, id);

        verifyLogHeader(
            logBuffer, toEventCodeId(REMOVE_IMAGE_CLEANUP), captureLength, captureLength, captureLength);
        assertEquals(sessionId, logBuffer.getInt(encodedMsgOffset(LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(streamId, logBuffer.getInt(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_INT), LITTLE_ENDIAN));
        assertEquals(id, logBuffer.getLong(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_INT * 2), LITTLE_ENDIAN));
        assertEquals(uri, logBuffer.getStringAscii(
            encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_INT * 2 + SIZE_OF_LONG), LITTLE_ENDIAN));
    }
}

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static io.aeron.agent.ArchiveEventCode.CMD_OUT_RESPONSE;
import static io.aeron.agent.ArchiveEventCode.EVENT_CODE_TYPE;
import static io.aeron.agent.ArchiveEventLogger.CONTROL_REQUEST_EVENTS;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.*;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.junit.jupiter.params.provider.EnumSource.Mode.INCLUDE;

class ArchiveEventLoggerTest
{
    @ParameterizedTest
    @EnumSource(ArchiveEventCode.class)
    void toEventCodeId(final ArchiveEventCode code)
    {
        assertEquals(0xFFFF + EVENT_CODE_TYPE + code.id(), ArchiveEventLogger.toEventCodeId(code));
    }

    @Test
    void logControlResponse()
    {
        final UnsafeBuffer dest = new UnsafeBuffer(allocateDirect(2048 + TRAILER_LENGTH));
        final ArchiveEventLogger logger = new ArchiveEventLogger(new ManyToOneRingBuffer(dest));
        final UnsafeBuffer src = new UnsafeBuffer(allocateDirect(128));
        src.setMemory(0, 64, (byte)1);

        logger.logControlResponse(src, 64);

        assertEquals(64 + HEADER_LENGTH, dest.getInt(lengthOffset(0), LITTLE_ENDIAN));
        assertEquals(ArchiveEventLogger.toEventCodeId(CMD_OUT_RESPONSE), dest.getInt(typeOffset(0), LITTLE_ENDIAN));
        final byte[] data = new byte[64];
        dest.getBytes(encodedMsgOffset(0), data);
        for (final byte b : data)
        {
            assertEquals((byte)1, b);
        }
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveEventCode.class, mode = EXCLUDE, names = { "CMD_OUT_RESPONSE" })
    void controlRequestEvents(final ArchiveEventCode code)
    {
        assertTrue(CONTROL_REQUEST_EVENTS.contains(code));
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveEventCode.class, mode = INCLUDE, names = { "CMD_OUT_RESPONSE" })
    void nonControlRequestEvents(final ArchiveEventCode code)
    {
        assertFalse(CONTROL_REQUEST_EVENTS.contains(code));
    }
}

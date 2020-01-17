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
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.fill;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.junit.jupiter.api.Assertions.*;

class CommonEventEncoderTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(MAX_EVENT_LENGTH, CACHE_LINE_LENGTH));

    @Test
    void encodeLogHeader()
    {
        final int encodedLength = internalEncodeLogHeader(buffer, 100, Integer.MAX_VALUE, () -> 555_000L);

        assertEquals(LOG_HEADER_LENGTH, encodedLength);
        assertEquals(100, buffer.getInt(0, LITTLE_ENDIAN));
        assertEquals(Integer.MAX_VALUE, buffer.getInt(SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(555_000L, buffer.getLong(SIZE_OF_INT * 2, LITTLE_ENDIAN));
    }

    @Test
    void encodeSocketAddress()
    {
        final InetSocketAddress socketAddress = new InetSocketAddress(15015);
        final byte[] address = socketAddress.getAddress().getAddress();

        final int encodedLength = CommonEventEncoder.encodeSocketAddress(buffer, 4, socketAddress);

        assertEquals(SIZE_OF_INT * 2 + address.length, encodedLength);
        assertEquals(15015, buffer.getInt(4, LITTLE_ENDIAN));
        assertEquals(address.length, buffer.getInt(4 + SIZE_OF_INT, LITTLE_ENDIAN));
        final byte[] encodedAddress = new byte[address.length];
        buffer.getBytes(4 + SIZE_OF_INT * 2, encodedAddress);
        assertArrayEquals(address, encodedAddress);
    }

    @Test
    void encodeTrailingStringAsAsciiWhenPayloadIsSmallerThanMaxMessageSizeWithoutHeader()
    {
        final int encodedLength = encodeTrailingString(buffer, LOG_HEADER_LENGTH, "ab©d️");

        assertEquals(SIZE_OF_INT + 5, encodedLength);
        assertEquals("ab?d?", buffer.getStringAscii(LOG_HEADER_LENGTH));
    }

    @Test
    void encodeTrailingStringAsAsciiWhenPayloadExceedsMaxMessageSizeWithoutHeader()
    {
        final int offset = 100;
        final char[] chars = new char[MAX_EVENT_LENGTH - offset - SIZE_OF_INT + 1];
        fill(chars, 'x');
        final String value = new String(chars);

        final int encodedLength = encodeTrailingString(buffer, offset, value);

        assertEquals(MAX_EVENT_LENGTH - offset, encodedLength);
        assertEquals(value.substring(0, MAX_EVENT_LENGTH - offset - SIZE_OF_INT - 3) + "...",
            buffer.getStringAscii(offset));
    }

    @Test
    void encodeBufferSmallerThanMaxCaptureSize()
    {
        final int offset = 20;
        final int length = 128;
        final UnsafeBuffer src = new UnsafeBuffer(allocateDirectAligned(256, CACHE_LINE_LENGTH));
        src.setMemory(offset, length, (byte)111);

        final int encodedLength = encode(buffer, src, offset, length);

        assertEquals(LOG_HEADER_LENGTH + length, encodedLength);
        assertEquals(length, buffer.getInt(0, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(SIZE_OF_INT * 2, LITTLE_ENDIAN));
        for (int i = 0; i < length; i++)
        {
            assertEquals(111, buffer.getByte(LOG_HEADER_LENGTH + i));
        }
    }

    @Test
    void encodeBufferBuggerThanMaxCaptureSize()
    {
        final int offset = 20;
        final int length = MAX_EVENT_LENGTH + 1000;
        final UnsafeBuffer src = new UnsafeBuffer(allocateDirectAligned(MAX_EVENT_LENGTH * 2, CACHE_LINE_LENGTH));
        src.setMemory(offset, length, (byte)-5);

        final int encodedLength = encode(buffer, src, offset, length);

        assertEquals(MAX_EVENT_LENGTH, encodedLength);
        assertEquals(MAX_CAPTURE_LENGTH, buffer.getInt(0, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(SIZE_OF_INT * 2, LITTLE_ENDIAN));
        for (int i = 0; i < MAX_CAPTURE_LENGTH; i++)
        {
            assertEquals(-5, buffer.getByte(LOG_HEADER_LENGTH + i));
        }
    }
}
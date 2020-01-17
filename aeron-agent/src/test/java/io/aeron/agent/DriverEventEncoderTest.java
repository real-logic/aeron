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

import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.DriverEventEncoder.*;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.fill;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class DriverEventEncoderTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirect(MAX_EVENT_LENGTH));

    @Test
    void encodePublicationRemovalShouldWriteUriLast()
    {
        final String uri = "aeron:udp?endpoint=224.10.9.8";
        final int expectedLength = 3 * SIZE_OF_INT + uri.length();

        final int encodedLength = encodePublicationRemoval(buffer, uri, 42, 5);

        assertEquals(LOG_HEADER_LENGTH + expectedLength, encodedLength);
        assertEquals(expectedLength, buffer.getInt(0, LITTLE_ENDIAN));
        assertEquals(expectedLength, buffer.getInt(SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(42, buffer.getInt(LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(5, buffer.getInt(LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(uri, buffer.getStringAscii(LOG_HEADER_LENGTH + SIZE_OF_INT * 2, LITTLE_ENDIAN));
    }

    @Test
    void encodePublicationRemovalShouldWriteOutAnEntireUriWhenFitsIntoTheResultMessage()
    {
        final char[] data = new char[MAX_EVENT_LENGTH - LOG_HEADER_LENGTH - 3 * SIZE_OF_INT];
        fill(data, 'x');
        final String uri = new String(data);

        final int encodedLength = encodePublicationRemoval(buffer, uri, 555, 222);

        assertEquals(MAX_EVENT_LENGTH, encodedLength);
        assertEquals(MAX_EVENT_LENGTH - LOG_HEADER_LENGTH, buffer.getInt(0, LITTLE_ENDIAN));
        assertEquals(MAX_EVENT_LENGTH - LOG_HEADER_LENGTH, buffer.getInt(SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(555, buffer.getInt(LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(222, buffer.getInt(LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(uri, buffer.getStringAscii(LOG_HEADER_LENGTH + SIZE_OF_INT * 2, LITTLE_ENDIAN));
    }

    @Test
    void encodePublicationRemovalShouldTruncateUriIfItExceedsMaxMessageLength()
    {
        final int encodedUriLength = MAX_EVENT_LENGTH - LOG_HEADER_LENGTH - 3 * SIZE_OF_INT;
        final char[] data = new char[encodedUriLength + 100];
        fill(data, 'z');
        final String uri = new String(data);

        final int encodedLength = encodePublicationRemoval(buffer, uri, 1, -1);

        assertEquals(MAX_EVENT_LENGTH, encodedLength);
        assertEquals(MAX_EVENT_LENGTH - LOG_HEADER_LENGTH, buffer.getInt(0, LITTLE_ENDIAN));
        assertEquals(3 * SIZE_OF_INT + data.length, buffer.getInt(SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(1, buffer.getInt(LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(-1, buffer.getInt(LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(uri.substring(0, encodedUriLength - 3) + "...",
            buffer.getStringAscii(LOG_HEADER_LENGTH + SIZE_OF_INT * 2, LITTLE_ENDIAN));
    }

    @Test
    void encodeSubscriptionRemovalShouldWriteUriLast()
    {
        final String uri = "aeron:udp?endpoint=224.10.9.8";
        final int expectedLength = 2 * SIZE_OF_INT + SIZE_OF_LONG + uri.length();

        final int encodedLength = encodeSubscriptionRemoval(buffer, uri, 13, Long.MAX_VALUE);

        assertEquals(LOG_HEADER_LENGTH + expectedLength, encodedLength);
        assertEquals(expectedLength, buffer.getInt(0, LITTLE_ENDIAN));
        assertEquals(expectedLength, buffer.getInt(SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(13, buffer.getInt(LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(Long.MAX_VALUE, buffer.getLong(LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(uri, buffer.getStringAscii(LOG_HEADER_LENGTH + SIZE_OF_INT + SIZE_OF_LONG, LITTLE_ENDIAN));
    }

    @Test
    void encodeSubscriptionRemovalShouldTruncateUriIfItExceedsMaxMessageLength()
    {
        final int encodedUriLength = MAX_EVENT_LENGTH - LOG_HEADER_LENGTH - 2 * SIZE_OF_INT - SIZE_OF_LONG;
        final char[] data = new char[encodedUriLength + 5];
        fill(data, 'a');
        final String uri = new String(data);

        final int encodedLength = encodeSubscriptionRemoval(buffer, uri, 1, -1);

        assertEquals(MAX_EVENT_LENGTH, encodedLength);
        assertEquals(MAX_EVENT_LENGTH - LOG_HEADER_LENGTH, buffer.getInt(0, LITTLE_ENDIAN));
        assertEquals(2 * SIZE_OF_INT + SIZE_OF_LONG + data.length, buffer.getInt(SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(1, buffer.getInt(LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(-1, buffer.getLong(LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(uri.substring(0, encodedUriLength - 3) + "...",
            buffer.getStringAscii(LOG_HEADER_LENGTH + SIZE_OF_INT + SIZE_OF_LONG, LITTLE_ENDIAN));
    }

    @Test
    void encodeImageRemovalShouldWriteUriLast()
    {
        final String uri = "aeron:udp?endpoint=224.10.9.8";
        final int expectedLength = 3 * SIZE_OF_INT + SIZE_OF_LONG + uri.length();

        final int encodedLength = encodeImageRemoval(buffer, uri, 13, 42, Long.MAX_VALUE);

        assertEquals(LOG_HEADER_LENGTH + expectedLength, encodedLength);
        assertEquals(expectedLength, buffer.getInt(0, LITTLE_ENDIAN));
        assertEquals(expectedLength, buffer.getInt(SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(13, buffer.getInt(LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(42, buffer.getInt(LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(Long.MAX_VALUE, buffer.getLong(LOG_HEADER_LENGTH + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(uri, buffer.getStringAscii(LOG_HEADER_LENGTH + SIZE_OF_INT * 2 + SIZE_OF_LONG, LITTLE_ENDIAN));
    }

    @Test
    void encodeImageRemovalShouldTruncateUriIfItExceedsMaxMessageLength()
    {
        final int encodedUriLength = MAX_EVENT_LENGTH - LOG_HEADER_LENGTH - 3 * SIZE_OF_INT - SIZE_OF_LONG;
        final char[] data = new char[encodedUriLength + 8];
        fill(data, 'a');
        final String uri = new String(data);

        final int encodedLength = encodeImageRemoval(buffer, uri, -1, 1, 0);

        assertEquals(MAX_EVENT_LENGTH, encodedLength);
        assertEquals(MAX_EVENT_LENGTH - LOG_HEADER_LENGTH, buffer.getInt(0, LITTLE_ENDIAN));
        assertEquals(3 * SIZE_OF_INT + SIZE_OF_LONG + data.length, buffer.getInt(SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(-1, buffer.getInt(LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(1, buffer.getInt(LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(0, buffer.getLong(LOG_HEADER_LENGTH + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(uri.substring(0, encodedUriLength - 3) + "...",
            buffer.getStringAscii(LOG_HEADER_LENGTH + SIZE_OF_INT * 2 + SIZE_OF_LONG, LITTLE_ENDIAN));
    }

}
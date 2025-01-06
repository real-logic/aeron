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
package io.aeron.logbuffer;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteOrder;
import java.util.Arrays;

import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class HeaderWriterTest
{
    private final UnsafeBuffer defaultHeaderBuffer = new UnsafeBuffer(new byte[32]);
    private final UnsafeBuffer termBuffer = new UnsafeBuffer(new byte[1024]);

    @BeforeEach
    void before()
    {
        Arrays.fill(defaultHeaderBuffer.byteArray(), (byte)-1);
        Arrays.fill(termBuffer.byteArray(), (byte)-1);
    }

    @ParameterizedTest
    @CsvSource({
        "100,8,5,9,352,-777,-1000,-33",
        "-99,-2,7,1,8,42,3,89",
        "123,0,0,0,0,0,0,0",
        "32,1,-128,4,96,2147483647,-2147483648,-2147483648",
    })
    @EnabledIf("littleEndian")
    void shouldEncodeHeaderUsingLittleEndianByteOrder(
        final int frameLength,
        final byte version,
        final byte flags,
        final short headerType,
        final int termOffset,
        final int sessionId,
        final int streamId,
        final int termId)
    {
        defaultHeaderBuffer.putByte(VERSION_FIELD_OFFSET, version);
        defaultHeaderBuffer.putByte(FLAGS_FIELD_OFFSET, flags);
        defaultHeaderBuffer.putShort(TYPE_FIELD_OFFSET, headerType, LITTLE_ENDIAN);
        defaultHeaderBuffer.putInt(SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);
        defaultHeaderBuffer.putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        final HeaderWriter headerWriter = HeaderWriter.newInstance(defaultHeaderBuffer);
        assertEquals(HeaderWriter.class, headerWriter.getClass());
        headerWriter.write(termBuffer, termOffset, frameLength, termId);

        assertEquals(-frameLength, termBuffer.getInt(termOffset + FRAME_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN));
        assertEquals(version, termBuffer.getByte(termOffset + VERSION_FIELD_OFFSET));
        assertEquals(flags, termBuffer.getByte(termOffset + FLAGS_FIELD_OFFSET));
        assertEquals(headerType, termBuffer.getShort(termOffset + TYPE_FIELD_OFFSET, LITTLE_ENDIAN));
        assertEquals(termOffset, termBuffer.getInt(termOffset + TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN));
        assertEquals(sessionId, termBuffer.getInt(termOffset + SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN));
        assertEquals(streamId, termBuffer.getInt(termOffset + STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN));
        assertEquals(termId, termBuffer.getInt(termOffset + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN));
    }

    @ParameterizedTest
    @CsvSource({
        "100,8,5,9,352,-777,-1000,-33",
        "-99,-2,7,1,8,42,3,89",
        "123,0,0,0,0,0,0,0",
        "32,1,-128,4,96,2147483647,-2147483648,-2147483648",
    })
    @EnabledIf("bigEndian")
    void shouldEncodeHeaderUsingBigEndianByteOrder(
        final int frameLength,
        final byte version,
        final byte flags,
        final short headerType,
        final int termOffset,
        final int sessionId,
        final int streamId,
        final int termId)
    {
        defaultHeaderBuffer.putByte(VERSION_FIELD_OFFSET, version);
        defaultHeaderBuffer.putByte(FLAGS_FIELD_OFFSET, flags);
        defaultHeaderBuffer.putShort(TYPE_FIELD_OFFSET, headerType, BIG_ENDIAN);
        defaultHeaderBuffer.putInt(SESSION_ID_FIELD_OFFSET, sessionId, BIG_ENDIAN);
        defaultHeaderBuffer.putInt(STREAM_ID_FIELD_OFFSET, streamId, BIG_ENDIAN);

        final HeaderWriter headerWriter = HeaderWriter.newInstance(defaultHeaderBuffer);
        assertNotEquals(HeaderWriter.class, headerWriter.getClass());
        headerWriter.write(termBuffer, termOffset, frameLength, termId);

        assertEquals(-frameLength, termBuffer.getInt(termOffset + FRAME_LENGTH_FIELD_OFFSET, BIG_ENDIAN));
        assertEquals(version, termBuffer.getByte(termOffset + VERSION_FIELD_OFFSET));
        assertEquals(flags, termBuffer.getByte(termOffset + FLAGS_FIELD_OFFSET));
        assertEquals(headerType, termBuffer.getShort(termOffset + TYPE_FIELD_OFFSET, BIG_ENDIAN));
        assertEquals(termOffset, termBuffer.getInt(termOffset + TERM_OFFSET_FIELD_OFFSET, BIG_ENDIAN));
        assertEquals(sessionId, termBuffer.getInt(termOffset + SESSION_ID_FIELD_OFFSET, BIG_ENDIAN));
        assertEquals(streamId, termBuffer.getInt(termOffset + STREAM_ID_FIELD_OFFSET, BIG_ENDIAN));
        assertEquals(termId, termBuffer.getInt(termOffset + TERM_ID_FIELD_OFFSET, BIG_ENDIAN));
    }

    private static boolean littleEndian()
    {
        return ByteOrder.nativeOrder() == LITTLE_ENDIAN;
    }

    private static boolean bigEndian()
    {
        return ByteOrder.nativeOrder() == BIG_ENDIAN;
    }
}

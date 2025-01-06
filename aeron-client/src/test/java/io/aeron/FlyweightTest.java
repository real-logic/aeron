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
package io.aeron;

import org.junit.jupiter.api.Test;
import io.aeron.command.PublicationMessageFlyweight;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.NakFlyweight;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlyweightTest
{
    private final ByteBuffer buffer = ByteBuffer.allocate(512);

    private final UnsafeBuffer aBuff = new UnsafeBuffer(buffer);
    private final HeaderFlyweight encodeHeader = new HeaderFlyweight();
    private final HeaderFlyweight decodeHeader = new HeaderFlyweight();
    private final DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
    private final DataHeaderFlyweight decodeDataHeader = new DataHeaderFlyweight();
    private final PublicationMessageFlyweight encodePublication = new PublicationMessageFlyweight();
    private final PublicationMessageFlyweight decodePublication = new PublicationMessageFlyweight();
    private final NakFlyweight encodeNakHeader = new NakFlyweight();
    private final NakFlyweight decodeNakHeader = new NakFlyweight();

    @Test
    void shouldWriteCorrectValuesForGenericHeaderFields()
    {
        encodeHeader.wrap(aBuff);

        encodeHeader.version((short)1);
        encodeHeader.flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
        encodeHeader.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        encodeHeader.frameLength(8);

        // little endian
        assertEquals((byte)0x08, buffer.get(0));
        assertEquals((byte)0x00, buffer.get(1));
        assertEquals((byte)0x00, buffer.get(2));
        assertEquals((byte)0x00, buffer.get(3));
        assertEquals((byte)0x01, buffer.get(4));
        assertEquals((byte)0xC0, buffer.get(5));
        assertEquals(HeaderFlyweight.HDR_TYPE_DATA, buffer.get(6));
        assertEquals((byte)0x00, buffer.get(7));
    }

    @Test
    void shouldReadWhatIsWrittenToGenericHeaderFields()
    {
        encodeHeader.wrap(aBuff);

        encodeHeader.version((short)1);
        encodeHeader.flags((short)0);
        encodeHeader.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        encodeHeader.frameLength(8);

        decodeHeader.wrap(aBuff);
        assertEquals(1, decodeHeader.version());
        assertEquals(HeaderFlyweight.HDR_TYPE_DATA, decodeHeader.headerType());
        assertEquals(8, decodeHeader.frameLength());
    }

    @Test
    void shouldWriteAndReadMultipleFramesCorrectly()
    {
        encodeHeader.wrap(aBuff);

        encodeHeader.version((short)1);
        encodeHeader.flags((short)0);
        encodeHeader.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        encodeHeader.frameLength(8);

        encodeHeader.wrap(aBuff, 8, aBuff.capacity() - 8);
        encodeHeader.version((short)2);
        encodeHeader.flags((short)0x01);
        encodeHeader.headerType(HeaderFlyweight.HDR_TYPE_SM);
        encodeHeader.frameLength(8);

        decodeHeader.wrap(aBuff);
        assertEquals(1, decodeHeader.version());
        assertEquals(0, decodeHeader.flags());
        assertEquals(HeaderFlyweight.HDR_TYPE_DATA, decodeHeader.headerType());
        assertEquals(8, decodeHeader.frameLength());

        decodeHeader.wrap(aBuff, 8, aBuff.capacity() - 8);
        assertEquals(2, decodeHeader.version());
        assertEquals(0x01, decodeHeader.flags());
        assertEquals(HeaderFlyweight.HDR_TYPE_SM, decodeHeader.headerType());
        assertEquals(8, decodeHeader.frameLength());
    }

    @Test
    void shouldReadAndWriteDataHeaderCorrectly()
    {
        encodeDataHeader.wrap(aBuff);

        encodeDataHeader.version((short)1);
        encodeDataHeader.flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
        encodeDataHeader.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        encodeDataHeader.frameLength(DataHeaderFlyweight.HEADER_LENGTH);
        encodeDataHeader.sessionId(0xdeadbeef);
        encodeDataHeader.streamId(0x44332211);
        encodeDataHeader.termId(0x99887766);

        decodeDataHeader.wrap(aBuff);
        assertEquals(1, decodeDataHeader.version());
        assertEquals(DataHeaderFlyweight.BEGIN_AND_END_FLAGS, decodeDataHeader.flags());
        assertEquals(HeaderFlyweight.HDR_TYPE_DATA, decodeDataHeader.headerType());
        assertEquals(DataHeaderFlyweight.HEADER_LENGTH, decodeDataHeader.frameLength());
        assertEquals(0xdeadbeef, decodeDataHeader.sessionId());
        assertEquals(0x44332211, decodeDataHeader.streamId());
        assertEquals(0x99887766, decodeDataHeader.termId());
        assertEquals(DataHeaderFlyweight.HEADER_LENGTH, decodeDataHeader.dataOffset());
    }

    @Test
    void shouldEncodeAndDecodeNakCorrectly()
    {
        encodeNakHeader.wrap(aBuff);
        encodeNakHeader.version((short)1);
        encodeNakHeader.flags((byte)0);
        encodeNakHeader.headerType(HeaderFlyweight.HDR_TYPE_NAK);
        encodeNakHeader.frameLength(NakFlyweight.HEADER_LENGTH);
        encodeNakHeader.sessionId(0xdeadbeef);
        encodeNakHeader.streamId(0x44332211);
        encodeNakHeader.termId(0x99887766);
        encodeNakHeader.termOffset(0x22334);
        encodeNakHeader.length(512);

        decodeNakHeader.wrap(aBuff);
        assertEquals(1, decodeNakHeader.version());
        assertEquals(0, decodeNakHeader.flags());
        assertEquals(HeaderFlyweight.HDR_TYPE_NAK, decodeNakHeader.headerType());
        assertEquals(NakFlyweight.HEADER_LENGTH, decodeNakHeader.frameLength());
        assertEquals(0xdeadbeef, decodeNakHeader.sessionId());
        assertEquals(0x44332211, decodeNakHeader.streamId());
        assertEquals(0x99887766, decodeNakHeader.termId());
        assertEquals(0x22334, decodeNakHeader.termOffset());
        assertEquals(512, decodeNakHeader.length());
    }

    @Test
    void shouldEncodeAndDecodeChannelsCorrectly()
    {
        encodePublication.wrap(aBuff, 0);

        final String channel = "aeron:udp?endpoint=localhost:4000";
        encodePublication.channel(channel);

        decodePublication.wrap(aBuff, 0);

        assertEquals(channel, decodePublication.channel());
    }
}

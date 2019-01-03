/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron;

import org.junit.Test;
import io.aeron.command.PublicationMessageFlyweight;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.NakFlyweight;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class FlyweightTest
{
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(512);

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
    public void shouldWriteCorrectValuesForGenericHeaderFields()
    {
        encodeHeader.wrap(aBuff);

        encodeHeader.version((short)1);
        encodeHeader.flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
        encodeHeader.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        encodeHeader.frameLength(8);

        // little endian
        assertThat(buffer.get(0), is((byte)0x08));
        assertThat(buffer.get(1), is((byte)0x00));
        assertThat(buffer.get(2), is((byte)0x00));
        assertThat(buffer.get(3), is((byte)0x00));
        assertThat(buffer.get(4), is((byte)0x01));
        assertThat(buffer.get(5), is((byte)0xC0));
        assertThat(buffer.get(6), is((byte)HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(buffer.get(7), is((byte)0x00));
    }

    @Test
    public void shouldReadWhatIsWrittenToGenericHeaderFields()
    {
        encodeHeader.wrap(aBuff);

        encodeHeader.version((short)1);
        encodeHeader.flags((short)0);
        encodeHeader.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        encodeHeader.frameLength(8);

        decodeHeader.wrap(aBuff);
        assertThat(decodeHeader.version(), is((short)1));
        assertThat(decodeHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(decodeHeader.frameLength(), is(8));
    }

    @Test
    public void shouldWriteAndReadMultipleFramesCorrectly()
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
        assertThat(decodeHeader.version(), is((short)1));
        assertThat(decodeHeader.flags(), is((short)0));
        assertThat(decodeHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(decodeHeader.frameLength(), is(8));

        decodeHeader.wrap(aBuff, 8, aBuff.capacity() - 8);
        assertThat(decodeHeader.version(), is((short)2));
        assertThat(decodeHeader.flags(), is((short)0x01));
        assertThat(decodeHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
        assertThat(decodeHeader.frameLength(), is(8));
    }

    @Test
    public void shouldReadAndWriteDataHeaderCorrectly()
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
        assertThat(decodeDataHeader.version(), is((short)1));
        assertThat(decodeDataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(decodeDataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(decodeDataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(decodeDataHeader.sessionId(), is(0xdeadbeef));
        assertThat(decodeDataHeader.streamId(), is(0x44332211));
        assertThat(decodeDataHeader.termId(), is(0x99887766));
        assertThat(decodeDataHeader.dataOffset(), is(DataHeaderFlyweight.HEADER_LENGTH));
    }

    @Test
    public void shouldEncodeAndDecodeNakCorrectly()
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
        assertThat(decodeNakHeader.version(), is((short)1));
        assertThat(decodeNakHeader.flags(), is((short)0));
        assertThat(decodeNakHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_NAK));
        assertThat(decodeNakHeader.frameLength(), is(NakFlyweight.HEADER_LENGTH));
        assertThat(decodeNakHeader.sessionId(), is(0xdeadbeef));
        assertThat(decodeNakHeader.streamId(), is(0x44332211));
        assertThat(decodeNakHeader.termId(), is(0x99887766));
        assertThat(decodeNakHeader.termOffset(), is(0x22334));
        assertThat(decodeNakHeader.length(), is(512));
    }

    @Test
    public void shouldEncodeAndDecodeChannelsCorrectly()
    {
        encodePublication.wrap(aBuff, 0);

        final String channel = "aeron:udp?endpoint=localhost:4000";
        encodePublication.channel(channel);

        decodePublication.wrap(aBuff, 0);

        assertThat(decodePublication.channel(), is(channel));
    }
}

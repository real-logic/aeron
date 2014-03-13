/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.util;

import org.junit.Test;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class HeaderFlyweightTest
{
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(256);
    private final AtomicBuffer aBuff = new AtomicBuffer(buffer);
    private final HeaderFlyweight encodeHeader = new HeaderFlyweight();
    private final HeaderFlyweight decodeHeader = new HeaderFlyweight();
    private final DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
    private final DataHeaderFlyweight decodeDataHeader = new DataHeaderFlyweight();

    @Test
    public void shouldWriteCorrectValuesForGenericHeaderFields()
    {
        encodeHeader.reset(aBuff, 0);

        encodeHeader.version((byte)1);
        encodeHeader.headerType((short)HeaderFlyweight.HDR_TYPE_DATA);
        encodeHeader.frameLength(8);
        encodeHeader.sessionId(0xdeadbeefL);

        // little endian
        assertThat(buffer.get(0), is((byte)(0x1L << 4)));
        assertThat(buffer.get(1), is((byte)HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(buffer.get(2), is((byte)0x8));
        assertThat(buffer.get(3), is((byte)0x0));
        assertThat(buffer.get(4), is((byte)0xef));
        assertThat(buffer.get(5), is((byte)0xbe));
        assertThat(buffer.get(6), is((byte)0xad));
        assertThat(buffer.get(7), is((byte)0xde));
    }

    @Test
    public void shouldReadWhatIsWrittenToGenericHeaderFields()
    {
        encodeHeader.reset(aBuff, 0);

        encodeHeader.version((byte)1);
        encodeHeader.headerType((short)HeaderFlyweight.HDR_TYPE_DATA);
        encodeHeader.frameLength(8);
        encodeHeader.sessionId(0xdeadbeefL);

        decodeHeader.reset(aBuff, 0);
        assertThat(decodeHeader.version(), is((byte)1));
        assertThat(decodeHeader.headerType(), is((short)HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(decodeHeader.frameLength(), is(8));
        assertThat(decodeHeader.sessionId(), is(0xdeadbeefL));
    }

    @Test
    public void shouldWriteAndReadMultipleFramesCorrectly()
    {
        encodeHeader.reset(aBuff, 0);

        encodeHeader.version((byte)1);
        encodeHeader.headerType((short)HeaderFlyweight.HDR_TYPE_DATA);
        encodeHeader.frameLength(8);
        encodeHeader.sessionId(0xdeadbeefL);

        encodeHeader.reset(aBuff, 8);
        encodeHeader.version((byte)2);
        encodeHeader.headerType((short)HeaderFlyweight.HDR_TYPE_CONN);
        encodeHeader.frameLength(8);
        encodeHeader.sessionId(0x11223344L);

        decodeHeader.reset(aBuff, 0);
        assertThat(decodeHeader.version(), is((byte)1));
        assertThat(decodeHeader.headerType(), is((short)HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(decodeHeader.frameLength(), is(8));
        assertThat(decodeHeader.sessionId(), is(0xdeadbeefL));

        decodeHeader.reset(aBuff, 8);
        assertThat(decodeHeader.version(), is((byte)2));
        assertThat(decodeHeader.headerType(), is((short)HeaderFlyweight.HDR_TYPE_CONN));
        assertThat(decodeHeader.frameLength(), is(8));
        assertThat(decodeHeader.sessionId(), is(0x11223344L));
    }

    @Test
    public void shouldReadAndWriteDataHeaderCorrectly()
    {
        encodeDataHeader.reset(aBuff, 0);

        encodeDataHeader.version((byte)1);
        encodeDataHeader.headerType((short)HeaderFlyweight.HDR_TYPE_DATA);
        encodeDataHeader.frameLength(8);
        encodeDataHeader.sessionId(0xdeadbeefL);
        encodeDataHeader.channelId(0x44332211L);
        encodeDataHeader.termId(0x99887766L);

        decodeDataHeader.reset(aBuff, 0);
        assertThat(decodeDataHeader.version(), is((byte) 1));
        assertThat(decodeDataHeader.headerType(), is((short) HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(decodeDataHeader.frameLength(), is(8));
        assertThat(decodeDataHeader.sessionId(), is(0xdeadbeefL));
        assertThat(decodeDataHeader.channelId(), is(0x44332211L));
        assertThat(decodeDataHeader.termId(), is(0x99887766L));
        assertThat(decodeDataHeader.dataOffset(), is(20));
    }
}

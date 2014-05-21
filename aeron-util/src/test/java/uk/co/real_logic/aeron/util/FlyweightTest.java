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
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class FlyweightTest
{
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(512);
    private final AtomicBuffer aBuff = new AtomicBuffer(buffer);
    private final HeaderFlyweight encodeHeader = new HeaderFlyweight();
    private final HeaderFlyweight decodeHeader = new HeaderFlyweight();
    private final DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
    private final DataHeaderFlyweight decodeDataHeader = new DataHeaderFlyweight();
    private final ChannelMessageFlyweight encodeChannel = new ChannelMessageFlyweight();
    private final ChannelMessageFlyweight decodeChannel = new ChannelMessageFlyweight();
    private final ErrorHeaderFlyweight encodeErrorHeader = new ErrorHeaderFlyweight();
    private final ErrorHeaderFlyweight decodeErrorHeader = new ErrorHeaderFlyweight();
    private final NewBufferMessageFlyweight encodeNewBuffer = new NewBufferMessageFlyweight();
    private final NewBufferMessageFlyweight decodeNewBuffer = new NewBufferMessageFlyweight();

    @Test
    public void shouldWriteCorrectValuesForGenericHeaderFields()
    {
        encodeHeader.wrap(aBuff, 0);

        encodeHeader.version((short) 1);
        encodeHeader.flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
        encodeHeader.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        encodeHeader.frameLength(8);

        // little endian
        assertThat(buffer.get(0), is((byte)0x01));
        assertThat(buffer.get(1), is((byte)0xC0));
        assertThat(buffer.get(2), is((byte)HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(buffer.get(3), is((byte)0x00));
        assertThat(buffer.get(4), is((byte)0x08));
        assertThat(buffer.get(5), is((byte)0x00));
        assertThat(buffer.get(6), is((byte)0x00));
        assertThat(buffer.get(7), is((byte)0x00));
    }

    @Test
    public void shouldReadWhatIsWrittenToGenericHeaderFields()
    {
        encodeHeader.wrap(aBuff, 0);

        encodeHeader.version((short) 1);
        encodeHeader.flags((short)0);
        encodeHeader.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        encodeHeader.frameLength(8);

        decodeHeader.wrap(aBuff, 0);
        assertThat(decodeHeader.version(), is((short)1));
        assertThat(decodeHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(decodeHeader.frameLength(), is(8));
    }

    @Test
    public void shouldWriteAndReadMultipleFramesCorrectly()
    {
        encodeHeader.wrap(aBuff, 0);

        encodeHeader.version((short) 1);
        encodeHeader.flags((short) 0);
        encodeHeader.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        encodeHeader.frameLength(8);

        encodeHeader.wrap(aBuff, 8);
        encodeHeader.version((short) 2);
        encodeHeader.flags((short) 0x01);
        encodeHeader.headerType(HeaderFlyweight.HDR_TYPE_SM);
        encodeHeader.frameLength(8);

        decodeHeader.wrap(aBuff, 0);
        assertThat(decodeHeader.version(), is((short) 1));
        assertThat(decodeHeader.flags(), is((short) 0));
        assertThat(decodeHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(decodeHeader.frameLength(), is(8));

        decodeHeader.wrap(aBuff, 8);
        assertThat(decodeHeader.version(), is((short) 2));
        assertThat(decodeHeader.flags(), is((short) 0x01));
        assertThat(decodeHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
        assertThat(decodeHeader.frameLength(), is(8));
    }

    @Test
    public void shouldReadAndWriteDataHeaderCorrectly()
    {
        encodeDataHeader.wrap(aBuff, 0);

        encodeDataHeader.version((short) 1);
        encodeDataHeader.flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
        encodeDataHeader.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        encodeDataHeader.frameLength(DataHeaderFlyweight.HEADER_LENGTH);
        encodeDataHeader.sessionId(0xdeadbeefL);
        encodeDataHeader.channelId(0x44332211L);
        encodeDataHeader.termId(0x99887766L);

        decodeDataHeader.wrap(aBuff, 0);
        assertThat(decodeDataHeader.version(), is((short) 1));
        assertThat(decodeDataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(decodeDataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(decodeDataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(decodeDataHeader.sessionId(), is(0xdeadbeefL));
        assertThat(decodeDataHeader.channelId(), is(0x44332211L));
        assertThat(decodeDataHeader.termId(), is(0x99887766L));
        assertThat(decodeDataHeader.dataOffset(), is(DataHeaderFlyweight.HEADER_LENGTH));
    }

    @Test
    public void shouldEncodeAndDecodeStringsCorrectly()
    {
        encodeChannel.wrap(aBuff, 0);

        String example = "abcç̀漢字仮名交じり文";
        encodeChannel.destination(example);

        decodeChannel.wrap(aBuff, 0);

        assertThat(decodeChannel.destination(), is(example));
    }

    @Test
    public void shouldReadAndWriteErrorHeaderWithoutErrorStringCorrectly()
    {
        final ByteBuffer originalBuffer = ByteBuffer.allocateDirect(256);
        final AtomicBuffer originalAtomicBuffer = new AtomicBuffer(originalBuffer);

        encodeDataHeader.wrap(originalAtomicBuffer, 0);
        encodeDataHeader.version((short) 1);
        encodeDataHeader.flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
        encodeDataHeader.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        encodeDataHeader.frameLength(DataHeaderFlyweight.HEADER_LENGTH);
        encodeDataHeader.sessionId(0xdeadbeefL);
        encodeDataHeader.channelId(0x44332211L);
        encodeDataHeader.termId(0x99887766L);

        encodeErrorHeader.wrap(aBuff, 0);
        encodeErrorHeader.version((short) 1);
        encodeErrorHeader.flags((short) 0);
        encodeErrorHeader.headerType(HeaderFlyweight.HDR_TYPE_ERR);
        encodeErrorHeader.frameLength(encodeDataHeader.frameLength() + 12);
        encodeErrorHeader.offendingHeader(encodeDataHeader, encodeDataHeader.frameLength());

        decodeErrorHeader.wrap(aBuff, 0);
        assertThat(decodeErrorHeader.version(), is((short) 1));
        assertThat(decodeErrorHeader.flags(), is((short) 0));
        assertThat(decodeErrorHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_ERR));
        assertThat(decodeErrorHeader.frameLength(), is(encodeDataHeader.frameLength() +
                                                       ErrorHeaderFlyweight.HEADER_LENGTH));

        decodeDataHeader.wrap(aBuff, decodeErrorHeader.offendingHeaderOffset());
        assertThat(decodeDataHeader.version(), is((short) 1));
        assertThat(decodeDataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(decodeDataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(decodeDataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(decodeDataHeader.sessionId(), is(0xdeadbeefL));
        assertThat(decodeDataHeader.channelId(), is(0x44332211L));
        assertThat(decodeDataHeader.termId(), is(0x99887766L));
        assertThat(decodeDataHeader.dataOffset(), is(encodeDataHeader.frameLength() +
                                                     ErrorHeaderFlyweight.HEADER_LENGTH));
    }

    @Test
    public void shouldReadAndWriteErrorHeaderWithErrorStringCorrectly()
    {
        String errorString = "this is an error";
        final ByteBuffer originalBuffer = ByteBuffer.allocateDirect(256);
        final AtomicBuffer originalAtomicBuffer = new AtomicBuffer(originalBuffer);

        encodeDataHeader.wrap(originalAtomicBuffer, 0);
        encodeDataHeader.version((short) 1);
        encodeDataHeader.flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
        encodeDataHeader.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        encodeDataHeader.frameLength(DataHeaderFlyweight.HEADER_LENGTH);
        encodeDataHeader.sessionId(0xdeadbeefL);
        encodeDataHeader.channelId(0x44332211L);
        encodeDataHeader.termId(0x99887766L);

        encodeErrorHeader.wrap(aBuff, 0);
        encodeErrorHeader.version((short) 1);
        encodeErrorHeader.flags((short) 0);
        encodeErrorHeader.headerType(HeaderFlyweight.HDR_TYPE_ERR);
        encodeErrorHeader.frameLength(encodeDataHeader.frameLength() +
                                      ErrorHeaderFlyweight.HEADER_LENGTH +
                                      errorString.length());
        encodeErrorHeader.offendingHeader(encodeDataHeader, encodeDataHeader.frameLength());
        encodeErrorHeader.errorString(errorString.getBytes());

        decodeErrorHeader.wrap(aBuff, 0);
        assertThat(decodeErrorHeader.version(), is((short) 1));
        assertThat(decodeErrorHeader.flags(), is((short) 0));
        assertThat(decodeErrorHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_ERR));
        assertThat(decodeErrorHeader.frameLength(), is(encodeDataHeader.frameLength() +
                                                       ErrorHeaderFlyweight.HEADER_LENGTH +
                                                       errorString.length()));

        decodeDataHeader.wrap(aBuff, decodeErrorHeader.offendingHeaderOffset());
        assertThat(decodeDataHeader.version(), is((short) 1));
        assertThat(decodeDataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(decodeDataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(decodeDataHeader.frameLength(), is(encodeDataHeader.frameLength()));
        assertThat(decodeDataHeader.sessionId(), is(0xdeadbeefL));
        assertThat(decodeDataHeader.channelId(), is(0x44332211L));
        assertThat(decodeDataHeader.termId(), is(0x99887766L));
        assertThat(decodeDataHeader.dataOffset(), is(encodeDataHeader.frameLength() +
                                                     ErrorHeaderFlyweight.HEADER_LENGTH));

        assertThat(decodeErrorHeader.errorStringOffset(), is(encodeDataHeader.frameLength() +
                                                             ErrorHeaderFlyweight.HEADER_LENGTH));
        assertThat(decodeErrorHeader.errorStringLength(), is(errorString.length()));
        assertThat(decodeErrorHeader.errorStringAsBytes(), is(errorString.getBytes()));
    }

    @Test
    public void newBufferMessagesSupportMultipleVariableLengthFields()
    {
        // given the buffers are clean to begin with
        assertLengthFindsNonZeroedBytes(0);
        encodeNewBuffer.wrap(aBuff, 0);

        encodeNewBuffer.channelId(1L)
                       .sessionId(2L)
                       .termId(3L)
                       .bufferOffset(0, 1)
                       .bufferOffset(1, 2)
                       .bufferOffset(2, 3)
                       .bufferLength(0, 1)
                       .bufferLength(1, 2)
                       .bufferLength(2, 3)
                       .location(0, "def")
                       .location(1, "ghi")
                       .location(2, "jkl")
                       .location(3, "def")
                       .location(4, "ghi")
                       .location(5, "jkl")
                       .destination("abc")
                       ;

        assertLengthFindsNonZeroedBytes(encodeNewBuffer.length());
        decodeNewBuffer.wrap(aBuff, 0);

        assertThat(decodeNewBuffer.channelId(), is(1L));
        assertThat(decodeNewBuffer.sessionId(), is(2L));
        assertThat(decodeNewBuffer.termId(), is(3L));

        assertThat(decodeNewBuffer.bufferOffset(0),is(1));
        assertThat(decodeNewBuffer.bufferOffset(1),is(2));
        assertThat(decodeNewBuffer.bufferOffset(2),is(3));

        assertThat(decodeNewBuffer.bufferLength(0),is(1));
        assertThat(decodeNewBuffer.bufferLength(1),is(2));
        assertThat(decodeNewBuffer.bufferLength(2),is(3));

        assertThat(decodeNewBuffer.location(0),is("def"));
        assertThat(decodeNewBuffer.location(1),is("ghi"));
        assertThat(decodeNewBuffer.location(2),is("jkl"));

        assertThat(decodeNewBuffer.location(3),is("def"));
        assertThat(decodeNewBuffer.location(4),is("ghi"));
        assertThat(decodeNewBuffer.location(5),is("jkl"));

        assertThat(decodeNewBuffer.destination(), is("abc"));
    }

    private void assertLengthFindsNonZeroedBytes(final int length)
    {
        IntStream.range(aBuff.capacity() - 1, length)
                 .forEach(i ->
                 {
                     assertThat(aBuff.getByte(i), is((byte) 0));
                 });
    }
}

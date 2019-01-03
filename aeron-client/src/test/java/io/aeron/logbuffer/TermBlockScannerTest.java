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
package io.aeron.logbuffer;

import org.junit.Before;
import org.junit.Test;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.FrameDescriptor.typeOffset;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.lengthOffset;

public class TermBlockScannerTest
{
    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);

    @Before
    public void before()
    {
        when(termBuffer.capacity()).thenReturn(LogBufferDescriptor.TERM_MIN_LENGTH);
    }

    @Test
    public void shouldScanEmptyBuffer()
    {
        final int offset = 0;
        final int limit = termBuffer.capacity();

        final int newOffset = TermBlockScanner.scan(termBuffer, offset, limit);
        assertThat(newOffset, is(offset));
    }

    @Test
    public void shouldReadFirstMessage()
    {
        final int offset = 0;
        final int limit = termBuffer.capacity();
        final int messageLength = 50;
        final int alignedMessageLength = BitUtil.align(messageLength, FRAME_ALIGNMENT);

        when(termBuffer.getIntVolatile(lengthOffset(offset))).thenReturn(messageLength);
        when(termBuffer.getShort(typeOffset(offset))).thenReturn((short)HDR_TYPE_DATA);

        final int newOffset = TermBlockScanner.scan(termBuffer, offset, limit);
        assertThat(newOffset, is(alignedMessageLength));
    }

    @Test
    public void shouldReadBlockOfTwoMessages()
    {
        final int offset = 0;
        final int limit = termBuffer.capacity();
        final int messageLength = 50;
        final int alignedMessageLength = BitUtil.align(messageLength, FRAME_ALIGNMENT);

        when(termBuffer.getIntVolatile(lengthOffset(offset))).thenReturn(messageLength);
        when(termBuffer.getShort(typeOffset(offset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(alignedMessageLength))).thenReturn(messageLength);
        when(termBuffer.getShort(typeOffset(alignedMessageLength))).thenReturn((short)HDR_TYPE_DATA);

        final int newOffset = TermBlockScanner.scan(termBuffer, offset, limit);
        assertThat(newOffset, is(alignedMessageLength * 2));
    }

    @Test
    public void shouldReadBlockOfThreeMessagesThatFillBuffer()
    {
        final int offset = 0;
        final int limit = termBuffer.capacity();
        final int messageLength = 50;
        final int alignedMessageLength = BitUtil.align(messageLength, FRAME_ALIGNMENT);
        final int thirdMessageLength = limit - (alignedMessageLength * 2);

        when(termBuffer.getIntVolatile(lengthOffset(offset))).thenReturn(messageLength);
        when(termBuffer.getShort(typeOffset(offset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(alignedMessageLength))).thenReturn(messageLength);
        when(termBuffer.getShort(typeOffset(alignedMessageLength))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(alignedMessageLength * 2))).thenReturn(thirdMessageLength);
        when(termBuffer.getShort(typeOffset(alignedMessageLength * 2))).thenReturn((short)HDR_TYPE_DATA);

        final int newOffset = TermBlockScanner.scan(termBuffer, offset, limit);
        assertThat(newOffset, is(limit));
    }

    @Test
    public void shouldReadBlockOfTwoMessagesBecauseOfLimit()
    {
        final int offset = 0;
        final int messageLength = 50;
        final int alignedMessageLength = BitUtil.align(messageLength, FRAME_ALIGNMENT);
        final int limit = (alignedMessageLength * 2) + 1;

        when(termBuffer.getIntVolatile(lengthOffset(offset))).thenReturn(messageLength);
        when(termBuffer.getShort(typeOffset(offset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(alignedMessageLength))).thenReturn(messageLength);
        when(termBuffer.getShort(typeOffset(alignedMessageLength))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(alignedMessageLength * 2))).thenReturn(messageLength);
        when(termBuffer.getShort(typeOffset(alignedMessageLength * 2))).thenReturn((short)HDR_TYPE_DATA);

        final int newOffset = TermBlockScanner.scan(termBuffer, offset, limit);
        assertThat(newOffset, is(alignedMessageLength * 2));
    }

    @Test
    public void shouldFailToReadFirstMessageBecauseOfLimit()
    {
        final int offset = 0;
        final int messageLength = 50;
        final int alignedMessageLength = BitUtil.align(messageLength, FRAME_ALIGNMENT);
        final int limit = alignedMessageLength - 1;

        when(termBuffer.getIntVolatile(lengthOffset(offset))).thenReturn(messageLength);
        when(termBuffer.getShort(typeOffset(offset))).thenReturn((short)HDR_TYPE_DATA);

        final int newOffset = TermBlockScanner.scan(termBuffer, offset, limit);
        assertThat(newOffset, is(offset));
    }

    @Test
    public void shouldReadOneMessageOnLimit()
    {
        final int offset = 0;
        final int messageLength = 50;
        final int alignedMessageLength = BitUtil.align(messageLength, FRAME_ALIGNMENT);

        when(termBuffer.getIntVolatile(lengthOffset(offset))).thenReturn(messageLength);
        when(termBuffer.getShort(typeOffset(offset))).thenReturn((short)HDR_TYPE_DATA);

        final int newOffset = TermBlockScanner.scan(termBuffer, offset, alignedMessageLength);
        assertThat(newOffset, is(alignedMessageLength));
    }

    @Test
    public void shouldReadBlockOfOneMessageThenPadding()
    {
        final int offset = 0;
        final int limit = termBuffer.capacity();
        final int messageLength = 50;
        final int alignedMessageLength = BitUtil.align(messageLength, FRAME_ALIGNMENT);

        when(termBuffer.getIntVolatile(lengthOffset(offset))).thenReturn(messageLength);
        when(termBuffer.getShort(typeOffset(offset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(alignedMessageLength))).thenReturn(messageLength);
        when(termBuffer.getShort(typeOffset(alignedMessageLength))).thenReturn((short)HDR_TYPE_PAD);

        final int firstOffset = TermBlockScanner.scan(termBuffer, offset, limit);
        assertThat(firstOffset, is(alignedMessageLength));

        final int secondOffset = TermBlockScanner.scan(termBuffer, firstOffset, limit);
        assertThat(secondOffset, is(alignedMessageLength * 2));
    }
}
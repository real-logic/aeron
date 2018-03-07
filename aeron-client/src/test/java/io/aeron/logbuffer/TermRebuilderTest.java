/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import org.mockito.InOrder;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.lang.Integer.valueOf;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.mockito.Mockito.*;
import static io.aeron.logbuffer.FrameDescriptor.*;

public class TermRebuilderTest
{
    private static final int TERM_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;

    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);

    @Before
    public void setUp()
    {
        when(valueOf(termBuffer.capacity())).thenReturn(valueOf(TERM_BUFFER_CAPACITY));
    }

    @Test
    public void shouldInsertIntoEmptyBuffer()
    {
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        final int termOffset = 0;
        final int srcOffset = 0;
        final int length = 256;
        packet.putInt(srcOffset, length, LITTLE_ENDIAN);

        TermRebuilder.insert(termBuffer, termOffset, packet, length);

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).putBytes(
            termOffset + HEADER_LENGTH, packet, srcOffset + HEADER_LENGTH, length - HEADER_LENGTH);
        inOrder.verify(termBuffer).putLong(termOffset + 24, packet.getLong(24));
        inOrder.verify(termBuffer).putLong(termOffset + 16, packet.getLong(16));
        inOrder.verify(termBuffer).putLong(termOffset + 8, packet.getLong(8));
        inOrder.verify(termBuffer).putLongOrdered(termOffset, packet.getLong(0));
    }

    @Test
    public void shouldInsertLastFrameIntoBuffer()
    {
        final int frameLength = BitUtil.align(256, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final int tail = TERM_BUFFER_CAPACITY - frameLength;
        final int termOffset = tail;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocateDirect(frameLength));
        packet.putShort(typeOffset(srcOffset), (short)PADDING_FRAME_TYPE, LITTLE_ENDIAN);
        packet.putInt(srcOffset, frameLength, LITTLE_ENDIAN);

        TermRebuilder.insert(termBuffer, termOffset, packet, frameLength);

        verify(termBuffer).putBytes(
            tail + HEADER_LENGTH, packet, srcOffset + HEADER_LENGTH, frameLength - HEADER_LENGTH);
    }

    @Test
    public void shouldFillSingleGap()
    {
        final int frameLength = 50;
        final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final int tail = alignedFrameLength;
        final int termOffset = tail;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocateDirect(alignedFrameLength));

        TermRebuilder.insert(termBuffer, termOffset, packet, alignedFrameLength);

        verify(termBuffer).putBytes(
            tail + HEADER_LENGTH, packet, srcOffset + HEADER_LENGTH, alignedFrameLength - HEADER_LENGTH);
    }

    @Test
    public void shouldFillAfterAGap()
    {
        final int frameLength = 50;
        final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocateDirect(alignedFrameLength));
        final int termOffset = alignedFrameLength * 2;

        TermRebuilder.insert(termBuffer, termOffset, packet, alignedFrameLength);

        verify(termBuffer).putBytes(
            (alignedFrameLength * 2) + HEADER_LENGTH,
            packet,
            srcOffset + HEADER_LENGTH,
            alignedFrameLength - HEADER_LENGTH);
    }

    @Test
    public void shouldFillGapButNotMoveTailOrHwm()
    {
        final int frameLength = 50;
        final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocateDirect(alignedFrameLength));
        final int termOffset = alignedFrameLength * 2;

        TermRebuilder.insert(termBuffer, termOffset, packet, alignedFrameLength);

        verify(termBuffer).putBytes(
            (alignedFrameLength * 2) + HEADER_LENGTH,
            packet,
            srcOffset + HEADER_LENGTH,
            alignedFrameLength - HEADER_LENGTH);
    }
}

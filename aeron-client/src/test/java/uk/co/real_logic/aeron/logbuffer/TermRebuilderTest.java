/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.logbuffer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static java.lang.Integer.valueOf;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.*;

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
        when(termBuffer.getInt(0, LITTLE_ENDIAN)).thenReturn(length);

        TermRebuilder.insert(termBuffer, termOffset, packet, length);

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).putBytes(termOffset, packet, srcOffset, length);
        inOrder.verify(termBuffer).putIntOrdered(termOffset, length);
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

        when(termBuffer.getInt(tail, LITTLE_ENDIAN)).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(tail), LITTLE_ENDIAN)).thenReturn((short)PADDING_FRAME_TYPE);

        TermRebuilder.insert(termBuffer, termOffset, packet, frameLength);

        verify(termBuffer).putBytes(tail, packet, srcOffset, frameLength);
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

        when(termBuffer.getInt(0)).thenReturn(frameLength);
        when(termBuffer.getInt(alignedFrameLength, LITTLE_ENDIAN)).thenReturn(frameLength);
        when(termBuffer.getInt(alignedFrameLength * 2, LITTLE_ENDIAN)).thenReturn(frameLength);

        TermRebuilder.insert(termBuffer, termOffset, packet, alignedFrameLength);

        verify(termBuffer).putBytes(tail, packet, srcOffset, alignedFrameLength);
    }

    @Test
    public void shouldFillAfterAGap()
    {
        final int frameLength = 50;
        final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocateDirect(alignedFrameLength));
        final int termOffset = alignedFrameLength * 2;

        when(termBuffer.getInt(0, LITTLE_ENDIAN)).thenReturn(0);
        when(termBuffer.getInt(alignedFrameLength, LITTLE_ENDIAN)).thenReturn(frameLength);

        TermRebuilder.insert(termBuffer, termOffset, packet, alignedFrameLength);

        verify(termBuffer).putBytes(alignedFrameLength * 2, packet, srcOffset, alignedFrameLength);
    }

    @Test
    public void shouldFillGapButNotMoveTailOrHwm()
    {
        final int frameLength = 50;
        final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocateDirect(alignedFrameLength));
        final int termOffset = alignedFrameLength * 2;

        when(termBuffer.getInt(0, LITTLE_ENDIAN)).thenReturn(frameLength);
        when(termBuffer.getInt(alignedFrameLength, LITTLE_ENDIAN)).thenReturn(0);

        TermRebuilder.insert(termBuffer, termOffset, packet, alignedFrameLength);

        verify(termBuffer).putBytes(alignedFrameLength * 2, packet, srcOffset, alignedFrameLength);
    }
}

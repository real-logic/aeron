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
package uk.co.real_logic.aeron.common.concurrent.logbuffer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static java.lang.Integer.valueOf;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;

public class LogRebuilderTest
{
    private static final int TERM_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int META_DATA_BUFFER_CAPACITY = TERM_META_DATA_LENGTH;

    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);

    @Before
    public void setUp()
    {
        when(valueOf(termBuffer.capacity())).thenReturn(valueOf(TERM_BUFFER_CAPACITY));
    }

    @Test
    public void shouldInsertIntoEmptyBuffer()
    {
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(256));
        final int termOffset = 0;
        final int srcOffset = 0;
        final int length = 256;
        packet.putInt(lengthOffset(srcOffset), length, LITTLE_ENDIAN);
        when(termBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN)).thenReturn(length);

        LogRebuilder.insert(termBuffer, termOffset, packet, srcOffset, length);

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).putBytes(termOffset, packet, srcOffset, length);
        inOrder.verify(termBuffer).putIntOrdered(lengthOffset(termOffset), length);
    }

    @Test
    public void shouldInsertLastFrameIntoBuffer()
    {
        final int frameLength = BitUtil.align(256, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final int tail = TERM_BUFFER_CAPACITY - frameLength;
        final int termOffset = tail;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(frameLength));
        packet.putShort(typeOffset(srcOffset), (short)PADDING_FRAME_TYPE, LITTLE_ENDIAN);
        packet.putInt(lengthOffset(srcOffset), frameLength, LITTLE_ENDIAN);

        when(termBuffer.getInt(lengthOffset(tail), LITTLE_ENDIAN)).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(tail), LITTLE_ENDIAN)).thenReturn((short)PADDING_FRAME_TYPE);

        LogRebuilder.insert(termBuffer, termOffset, packet, srcOffset, frameLength);

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
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(alignedFrameLength));

        when(termBuffer.getInt(lengthOffset(0))).thenReturn(frameLength);
        when(termBuffer.getInt(lengthOffset(alignedFrameLength), LITTLE_ENDIAN)).thenReturn(frameLength);
        when(termBuffer.getInt(lengthOffset(alignedFrameLength * 2), LITTLE_ENDIAN)).thenReturn(frameLength);

        LogRebuilder.insert(termBuffer, termOffset, packet, srcOffset, alignedFrameLength);

        verify(termBuffer).putBytes(tail, packet, srcOffset, alignedFrameLength);
    }

    @Test
    public void shouldFillAfterAGap()
    {
        final int frameLength = 50;
        final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(alignedFrameLength));
        final int termOffset = alignedFrameLength * 2;

        when(termBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN)).thenReturn(0);
        when(termBuffer.getInt(lengthOffset(alignedFrameLength), LITTLE_ENDIAN)).thenReturn(frameLength);

        LogRebuilder.insert(termBuffer, termOffset, packet, srcOffset, alignedFrameLength);

        verify(termBuffer).putBytes(alignedFrameLength * 2, packet, srcOffset, alignedFrameLength);
    }

    @Test
    public void shouldFillGapButNotMoveTailOrHwm()
    {
        final int frameLength = 50;
        final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(alignedFrameLength));
        final int termOffset = alignedFrameLength * 2;

        when(termBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN)).thenReturn(frameLength);
        when(termBuffer.getInt(lengthOffset(alignedFrameLength), LITTLE_ENDIAN)).thenReturn(0);

        LogRebuilder.insert(termBuffer, termOffset, packet, srcOffset, alignedFrameLength);

        verify(termBuffer).putBytes(alignedFrameLength * 2, packet, srcOffset, alignedFrameLength);
    }
}

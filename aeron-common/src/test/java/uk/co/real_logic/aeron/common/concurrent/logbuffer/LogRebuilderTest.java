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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;

public class LogRebuilderTest
{
    private static final int TERM_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int META_DATA_BUFFER_CAPACITY = TERM_META_DATA_LENGTH;

    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);
    private final UnsafeBuffer metaDataBuffer = spy(new UnsafeBuffer(new byte[META_DATA_BUFFER_CAPACITY]));

    private LogRebuilder logRebuilder;

    @Before
    public void setUp()
    {
        when(valueOf(termBuffer.capacity())).thenReturn(valueOf(TERM_BUFFER_CAPACITY));

        logRebuilder = new LogRebuilder(termBuffer, metaDataBuffer);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCapacityNotMultipleOfAlignment()
    {
        final int logBufferCapacity = LogBufferDescriptor.TERM_MIN_LENGTH + FRAME_ALIGNMENT + 1;

        when(termBuffer.capacity()).thenReturn(logBufferCapacity);

        logRebuilder = new LogRebuilder(termBuffer, metaDataBuffer);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientMetaDataBufferCapacity()
    {
        when(metaDataBuffer.capacity()).thenReturn(LogBufferDescriptor.TERM_META_DATA_LENGTH - 1);

        logRebuilder = new LogRebuilder(termBuffer, metaDataBuffer);
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

        logRebuilder.insert(termOffset, packet, srcOffset, length);
        LogRebuilder.scanForCompletion(termBuffer, termOffset, TERM_BUFFER_CAPACITY);

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
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

        logRebuilder.insert(termOffset, packet, srcOffset, frameLength);
        assertThat(LogRebuilder.scanForCompletion(termBuffer, tail, TERM_BUFFER_CAPACITY), is(TERM_BUFFER_CAPACITY));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(termBuffer).putBytes(tail, packet, srcOffset, frameLength);
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

        logRebuilder.insert(termOffset, packet, srcOffset, alignedFrameLength);
        assertThat(LogRebuilder.scanForCompletion(termBuffer, tail, TERM_BUFFER_CAPACITY), is(alignedFrameLength * 3));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(termBuffer).putBytes(tail, packet, srcOffset, alignedFrameLength);
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

        logRebuilder.insert(termOffset, packet, srcOffset, alignedFrameLength);
        assertThat(LogRebuilder.scanForCompletion(termBuffer, 0, TERM_BUFFER_CAPACITY), is(0));

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

        logRebuilder.insert(termOffset, packet, srcOffset, alignedFrameLength);
        assertThat(LogRebuilder.scanForCompletion(termBuffer, alignedFrameLength, TERM_BUFFER_CAPACITY), is(alignedFrameLength));

        verify(termBuffer).putBytes(alignedFrameLength * 2, packet, srcOffset, alignedFrameLength);
    }
}

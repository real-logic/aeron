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
package uk.co.real_logic.aeron.common.concurrent.logbuffer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.common.BitUtil;
import uk.co.real_logic.aeron.common.concurrent.UnsafeBuffer;

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
    private static final int LOG_BUFFER_CAPACITY = LogBufferDescriptor.MIN_LOG_SIZE;
    private static final int STATE_BUFFER_CAPACITY = STATE_BUFFER_LENGTH;

    private final UnsafeBuffer logBuffer = mock(UnsafeBuffer.class);
    private final UnsafeBuffer stateBuffer = spy(new UnsafeBuffer(new byte[STATE_BUFFER_CAPACITY]));
    private final StateViewer stateViewer = new StateViewer(stateBuffer);

    private LogRebuilder logRebuilder;

    @Before
    public void setUp()
    {
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(LOG_BUFFER_CAPACITY));

        logRebuilder = new LogRebuilder(logBuffer, stateBuffer);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCapacityNotMultipleOfAlignment()
    {
        final int logBufferCapacity = LogBufferDescriptor.MIN_LOG_SIZE + FRAME_ALIGNMENT + 1;

        when(logBuffer.capacity()).thenReturn(logBufferCapacity);

        logRebuilder = new LogRebuilder(logBuffer, stateBuffer);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientStateBufferCapacity()
    {
        when(stateBuffer.capacity()).thenReturn(LogBufferDescriptor.STATE_BUFFER_LENGTH - 1);

        logRebuilder = new LogRebuilder(logBuffer, stateBuffer);
    }

    @Test
    public void shouldInsertIntoEmptyBuffer()
    {
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(256));
        final int srcOffset = 0;
        final int length = 256;

        when(logBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN)).thenReturn(length);

        logRebuilder.insert(packet, srcOffset, length);
        assertFalse(logRebuilder.isComplete());

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(logBuffer).putBytes(0, packet, srcOffset, length);
        inOrder.verify(stateBuffer).putIntOrdered(TAIL_COUNTER_OFFSET, length);
        inOrder.verify(stateBuffer).putIntOrdered(HIGH_WATER_MARK_OFFSET, length);
    }

    @Test
    public void shouldInsertLastFrameIntoBuffer()
    {
        final int frameLength = BitUtil.align(256, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final int tail = LOG_BUFFER_CAPACITY - frameLength;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(frameLength));
        packet.putShort(typeOffset(srcOffset), (short)PADDING_FRAME_TYPE, LITTLE_ENDIAN);
        packet.putInt(termOffsetOffset(srcOffset), tail, LITTLE_ENDIAN);
        packet.putInt(lengthOffset(srcOffset), frameLength, LITTLE_ENDIAN);

        when(stateBuffer.getInt(TAIL_COUNTER_OFFSET)).thenReturn(tail);
        when(stateBuffer.getInt(HIGH_WATER_MARK_OFFSET)).thenReturn(tail);
        when(logBuffer.getInt(lengthOffset(tail), LITTLE_ENDIAN)).thenReturn(frameLength);
        when(logBuffer.getShort(typeOffset(tail), LITTLE_ENDIAN)).thenReturn((short)PADDING_FRAME_TYPE);

        logRebuilder.insert(packet, srcOffset, frameLength);
        assertTrue(logRebuilder.isComplete());

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(logBuffer).putBytes(tail, packet, srcOffset, frameLength);
        inOrder.verify(stateBuffer).putIntOrdered(TAIL_COUNTER_OFFSET, tail + frameLength);
        inOrder.verify(stateBuffer).putIntOrdered(HIGH_WATER_MARK_OFFSET, tail + frameLength);
    }

    @Test
    public void shouldFillSingleGap()
    {
        final int frameLength = 50;
        final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final int tail = alignedFrameLength;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(alignedFrameLength));
        packet.putInt(termOffsetOffset(srcOffset), tail, LITTLE_ENDIAN);

        stateBuffer.putInt(TAIL_COUNTER_OFFSET, alignedFrameLength);
        stateBuffer.putInt(HIGH_WATER_MARK_OFFSET, alignedFrameLength * 3);
        when(logBuffer.getInt(lengthOffset(0))).thenReturn(frameLength);
        when(logBuffer.getInt(lengthOffset(alignedFrameLength), LITTLE_ENDIAN)).thenReturn(frameLength);
        when(logBuffer.getInt(lengthOffset(alignedFrameLength * 2), LITTLE_ENDIAN)).thenReturn(frameLength);

        logRebuilder.insert(packet, srcOffset, alignedFrameLength);

        assertThat(stateViewer.tailVolatile(), is(alignedFrameLength * 3));
        assertThat(stateViewer.highWaterMarkVolatile(), is(alignedFrameLength * 3));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(logBuffer).putBytes(tail, packet, srcOffset, alignedFrameLength);
        inOrder.verify(stateBuffer).putIntOrdered(TAIL_COUNTER_OFFSET, alignedFrameLength * 3);
    }

    @Test
    public void shouldFillAfterAGap()
    {
        final int frameLength = 50;
        final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(alignedFrameLength));
        packet.putInt(termOffsetOffset(srcOffset), alignedFrameLength * 2, LITTLE_ENDIAN);

        stateBuffer.putInt(TAIL_COUNTER_OFFSET, 0);
        stateBuffer.putInt(HIGH_WATER_MARK_OFFSET, alignedFrameLength * 2);
        when(logBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN)).thenReturn(0);
        when(logBuffer.getInt(lengthOffset(alignedFrameLength), LITTLE_ENDIAN)).thenReturn(frameLength);

        logRebuilder.insert(packet, srcOffset, alignedFrameLength);

        assertThat(stateViewer.tailVolatile(), is(0));
        assertThat(stateViewer.highWaterMarkVolatile(), is(alignedFrameLength * 3));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(logBuffer).putBytes(alignedFrameLength * 2, packet, srcOffset, alignedFrameLength);
        inOrder.verify(stateBuffer).putIntOrdered(HIGH_WATER_MARK_OFFSET, alignedFrameLength * 3);
    }

    @Test
    public void shouldFillGapButNotMoveTailOrHwm()
    {
        final int frameLength = 50;
        final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(alignedFrameLength));
        packet.putInt(termOffsetOffset(srcOffset), alignedFrameLength * 2, LITTLE_ENDIAN);

        stateBuffer.putInt(TAIL_COUNTER_OFFSET, alignedFrameLength);
        stateBuffer.putInt(HIGH_WATER_MARK_OFFSET, alignedFrameLength * 4);
        when(logBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN)).thenReturn(frameLength);
        when(logBuffer.getInt(lengthOffset(alignedFrameLength), LITTLE_ENDIAN)).thenReturn(0);

        logRebuilder.insert(packet, srcOffset, alignedFrameLength);

        assertThat(stateViewer.tailVolatile(), is(alignedFrameLength));
        assertThat(stateViewer.highWaterMarkVolatile(), is(alignedFrameLength * 4));

        verify(logBuffer).putBytes(alignedFrameLength * 2, packet, srcOffset, alignedFrameLength);
    }
}

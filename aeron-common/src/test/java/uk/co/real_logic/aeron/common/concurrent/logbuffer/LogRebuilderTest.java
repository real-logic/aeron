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
    private final MetaDataViewer metaDataViewer = new MetaDataViewer(metaDataBuffer);

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
        final int srcOffset = 0;
        final int length = 256;

        when(termBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN)).thenReturn(length);

        logRebuilder.insert(packet, srcOffset, length);
        assertFalse(logRebuilder.isComplete());

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(termBuffer).putBytes(0, packet, srcOffset, length);
        inOrder.verify(metaDataBuffer).putIntOrdered(TERM_TAIL_COUNTER_OFFSET, length);
        inOrder.verify(metaDataBuffer).putIntOrdered(TERM_HIGH_WATER_MARK_OFFSET, length);
    }

    @Test
    public void shouldInsertLastFrameIntoBuffer()
    {
        final int frameLength = BitUtil.align(256, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final int tail = TERM_BUFFER_CAPACITY - frameLength;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(frameLength));
        packet.putShort(typeOffset(srcOffset), (short)PADDING_FRAME_TYPE, LITTLE_ENDIAN);
        packet.putInt(termOffsetOffset(srcOffset), tail, LITTLE_ENDIAN);
        packet.putInt(lengthOffset(srcOffset), frameLength, LITTLE_ENDIAN);

        when(metaDataBuffer.getInt(TERM_TAIL_COUNTER_OFFSET)).thenReturn(tail);
        when(metaDataBuffer.getInt(TERM_HIGH_WATER_MARK_OFFSET)).thenReturn(tail);
        when(termBuffer.getInt(lengthOffset(tail), LITTLE_ENDIAN)).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(tail), LITTLE_ENDIAN)).thenReturn((short)PADDING_FRAME_TYPE);

        logRebuilder.insert(packet, srcOffset, frameLength);
        assertTrue(logRebuilder.isComplete());

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(termBuffer).putBytes(tail, packet, srcOffset, frameLength);
        inOrder.verify(metaDataBuffer).putIntOrdered(TERM_TAIL_COUNTER_OFFSET, tail + frameLength);
        inOrder.verify(metaDataBuffer).putIntOrdered(TERM_HIGH_WATER_MARK_OFFSET, tail + frameLength);
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

        metaDataBuffer.putInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        metaDataBuffer.putInt(TERM_HIGH_WATER_MARK_OFFSET, alignedFrameLength * 3);
        when(termBuffer.getInt(lengthOffset(0))).thenReturn(frameLength);
        when(termBuffer.getInt(lengthOffset(alignedFrameLength), LITTLE_ENDIAN)).thenReturn(frameLength);
        when(termBuffer.getInt(lengthOffset(alignedFrameLength * 2), LITTLE_ENDIAN)).thenReturn(frameLength);

        logRebuilder.insert(packet, srcOffset, alignedFrameLength);

        assertThat(metaDataViewer.tailVolatile(), is(alignedFrameLength * 3));
        assertThat(metaDataViewer.highWaterMarkVolatile(), is(alignedFrameLength * 3));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(termBuffer).putBytes(tail, packet, srcOffset, alignedFrameLength);
        inOrder.verify(metaDataBuffer).putIntOrdered(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength * 3);
    }

    @Test
    public void shouldFillAfterAGap()
    {
        final int frameLength = 50;
        final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(alignedFrameLength));
        packet.putInt(termOffsetOffset(srcOffset), alignedFrameLength * 2, LITTLE_ENDIAN);

        metaDataBuffer.putInt(TERM_TAIL_COUNTER_OFFSET, 0);
        metaDataBuffer.putInt(TERM_HIGH_WATER_MARK_OFFSET, alignedFrameLength * 2);
        when(termBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN)).thenReturn(0);
        when(termBuffer.getInt(lengthOffset(alignedFrameLength), LITTLE_ENDIAN)).thenReturn(frameLength);

        logRebuilder.insert(packet, srcOffset, alignedFrameLength);

        assertThat(metaDataViewer.tailVolatile(), is(0));
        assertThat(metaDataViewer.highWaterMarkVolatile(), is(alignedFrameLength * 3));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(termBuffer).putBytes(alignedFrameLength * 2, packet, srcOffset, alignedFrameLength);
        inOrder.verify(metaDataBuffer).putIntOrdered(TERM_HIGH_WATER_MARK_OFFSET, alignedFrameLength * 3);
    }

    @Test
    public void shouldFillGapButNotMoveTailOrHwm()
    {
        final int frameLength = 50;
        final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
        final int srcOffset = 0;
        final UnsafeBuffer packet = new UnsafeBuffer(ByteBuffer.allocate(alignedFrameLength));
        packet.putInt(termOffsetOffset(srcOffset), alignedFrameLength * 2, LITTLE_ENDIAN);

        metaDataBuffer.putInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        metaDataBuffer.putInt(TERM_HIGH_WATER_MARK_OFFSET, alignedFrameLength * 4);
        when(termBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN)).thenReturn(frameLength);
        when(termBuffer.getInt(lengthOffset(alignedFrameLength), LITTLE_ENDIAN)).thenReturn(0);

        logRebuilder.insert(packet, srcOffset, alignedFrameLength);

        assertThat(metaDataViewer.tailVolatile(), is(alignedFrameLength));
        assertThat(metaDataViewer.highWaterMarkVolatile(), is(alignedFrameLength * 4));

        verify(termBuffer).putBytes(alignedFrameLength * 2, packet, srcOffset, alignedFrameLength);
    }
}

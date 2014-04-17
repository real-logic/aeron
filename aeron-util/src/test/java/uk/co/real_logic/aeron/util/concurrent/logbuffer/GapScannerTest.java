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
package uk.co.real_logic.aeron.util.concurrent.logbuffer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.lengthOffset;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.HIGH_WATER_MARK_OFFSET;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.TAIL_COUNTER_OFFSET;

public class GapScannerTest
{
    private static final int LOG_BUFFER_CAPACITY = 1024 * 16;
    private static final int STATE_BUFFER_CAPACITY = STATE_BUFFER_LENGTH;

    private final AtomicBuffer logBuffer = mock(AtomicBuffer.class);
    private final AtomicBuffer stateBuffer = spy(new AtomicBuffer(new byte[STATE_BUFFER_CAPACITY]));
    private final GapScanner.GapHandler gapHandler = mock(GapScanner.GapHandler.class);

    private GapScanner scanner;

    @Before
    public void setUp()
    {
        when(logBuffer.capacity()).thenReturn(LOG_BUFFER_CAPACITY);

        scanner = new GapScanner(logBuffer, stateBuffer);
    }

    @Test
    public void shouldReportNoGapsOnEmptyBuffer()
    {
        assertThat(scanner.scan(gapHandler), is(0));

        final InOrder inOrder = inOrder(stateBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(HIGH_WATER_MARK_OFFSET);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        verifyZeroInteractions(gapHandler);
    }

    @Test
    public void shouldReportNoGapsWhenTailEqualsHighWaterMark()
    {
        final int fillLevel = FRAME_ALIGNMENT * 4;
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(fillLevel);
        when(stateBuffer.getIntVolatile(HIGH_WATER_MARK_OFFSET)).thenReturn(fillLevel);

        assertThat(scanner.scan(gapHandler), is(0));

        final InOrder inOrder = inOrder(stateBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(HIGH_WATER_MARK_OFFSET);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        verifyZeroInteractions(gapHandler);
    }

    @Test
    public void shouldReportGapAtBeginningOfBuffer()
    {
        final int frameOffset = FRAME_ALIGNMENT * 3;
        final int highWaterMark = frameOffset + FRAME_ALIGNMENT;
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(0);
        when(stateBuffer.getIntVolatile(HIGH_WATER_MARK_OFFSET)).thenReturn(highWaterMark);
        when(logBuffer.getInt(lengthOffset(frameOffset), LITTLE_ENDIAN)).thenReturn(FRAME_ALIGNMENT);

        assertThat(scanner.scan(gapHandler), is(1));

        final InOrder inOrder = inOrder(stateBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(HIGH_WATER_MARK_OFFSET);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        verify(gapHandler).onGap(logBuffer, 0, frameOffset);
    }

    @Test
    public void shouldReportSingleGapWhenBufferNotFull()
    {
        final int tail = FRAME_ALIGNMENT;
        final int highWaterMark = FRAME_ALIGNMENT * 3;
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(tail);
        when(stateBuffer.getIntVolatile(HIGH_WATER_MARK_OFFSET)).thenReturn(highWaterMark);

        when(logBuffer.getInt(lengthOffset(tail - FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getInt(lengthOffset(tail), LITTLE_ENDIAN))
            .thenReturn(0);
        when(logBuffer.getInt(lengthOffset(highWaterMark - FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);

        assertThat(scanner.scan(gapHandler), is(1));

        final InOrder inOrder = inOrder(stateBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(HIGH_WATER_MARK_OFFSET);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        verify(gapHandler).onGap(logBuffer, tail, FRAME_ALIGNMENT);
    }

    @Test
    public void shouldReportSingleGapWhenBufferIsFull()
    {
        final int tail = LOG_BUFFER_CAPACITY - (FRAME_ALIGNMENT * 2);
        final int highWaterMark = LOG_BUFFER_CAPACITY;
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(tail);
        when(stateBuffer.getIntVolatile(HIGH_WATER_MARK_OFFSET)).thenReturn(highWaterMark);

        when(logBuffer.getInt(lengthOffset(tail - FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getInt(lengthOffset(tail), LITTLE_ENDIAN))
            .thenReturn(0);
        when(logBuffer.getInt(lengthOffset(highWaterMark - FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);

        assertThat(scanner.scan(gapHandler), is(1));

        final InOrder inOrder = inOrder(stateBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(HIGH_WATER_MARK_OFFSET);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        verify(gapHandler).onGap(logBuffer, tail, FRAME_ALIGNMENT);
    }

    @Test
    public void shouldReportMultipleGaps()
    {
        final int tail = FRAME_ALIGNMENT;
        final int highWaterMark = FRAME_ALIGNMENT * 6;
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(tail);
        when(stateBuffer.getIntVolatile(HIGH_WATER_MARK_OFFSET)).thenReturn(highWaterMark);

        when(logBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getInt(lengthOffset(FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn(0);
        when(logBuffer.getInt(lengthOffset(FRAME_ALIGNMENT * 2), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getInt(lengthOffset(FRAME_ALIGNMENT * 3), LITTLE_ENDIAN))
            .thenReturn(0);
        when(logBuffer.getInt(lengthOffset(FRAME_ALIGNMENT * 5), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);

        assertThat(scanner.scan(gapHandler), is(2));

        final InOrder inOrder = inOrder(stateBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(HIGH_WATER_MARK_OFFSET);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        verify(gapHandler).onGap(logBuffer, FRAME_ALIGNMENT, FRAME_ALIGNMENT);
        verify(gapHandler).onGap(logBuffer, FRAME_ALIGNMENT * 3, FRAME_ALIGNMENT * 2);
    }
}

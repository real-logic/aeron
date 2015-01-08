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
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.lengthOffset;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;

public class GapScannerTest
{
    private static final int LOG_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int META_DATA_BUFFER_CAPACITY = TERM_META_DATA_LENGTH;

    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);
    private final UnsafeBuffer metaDataBuffer = mock(UnsafeBuffer.class);
    private final GapScanner.GapHandler gapHandler = mock(GapScanner.GapHandler.class);

    private GapScanner scanner;

    @Before
    public void setUp()
    {
        when(termBuffer.capacity()).thenReturn(LOG_BUFFER_CAPACITY);
        when(metaDataBuffer.capacity()).thenReturn(META_DATA_BUFFER_CAPACITY);

        scanner = new GapScanner(termBuffer, metaDataBuffer);
    }

    @Test
    public void shouldReportNoGapsOnEmptyBuffer()
    {
        assertThat(scanner.scan(gapHandler), is(0));

        final InOrder inOrder = inOrder(metaDataBuffer);
        inOrder.verify(metaDataBuffer).getIntVolatile(TERM_HIGH_WATER_MARK_OFFSET);
        inOrder.verify(metaDataBuffer).getIntVolatile(TERM_TAIL_COUNTER_OFFSET);
        verifyZeroInteractions(gapHandler);
    }

    @Test
    public void shouldReportNoGapsWhenTailEqualsHighWaterMark()
    {
        final int fillLevel = FRAME_ALIGNMENT * 4;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET)).thenReturn(fillLevel);
        when(metaDataBuffer.getIntVolatile(TERM_HIGH_WATER_MARK_OFFSET)).thenReturn(fillLevel);

        assertThat(scanner.scan(gapHandler), is(0));

        final InOrder inOrder = inOrder(metaDataBuffer);
        inOrder.verify(metaDataBuffer).getIntVolatile(TERM_HIGH_WATER_MARK_OFFSET);
        inOrder.verify(metaDataBuffer).getIntVolatile(TERM_TAIL_COUNTER_OFFSET);
        verifyZeroInteractions(gapHandler);
    }

    @Test
    public void shouldReportGapAtBeginningOfBuffer()
    {
        final int frameOffset = FRAME_ALIGNMENT * 3;
        final int highWaterMark = frameOffset + FRAME_ALIGNMENT;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET)).thenReturn(0);
        when(metaDataBuffer.getIntVolatile(TERM_HIGH_WATER_MARK_OFFSET)).thenReturn(highWaterMark);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset))).thenReturn(FRAME_ALIGNMENT);

        assertThat(scanner.scan(gapHandler), is(1));

        final InOrder inOrder = inOrder(metaDataBuffer);
        inOrder.verify(metaDataBuffer).getIntVolatile(TERM_HIGH_WATER_MARK_OFFSET);
        inOrder.verify(metaDataBuffer).getIntVolatile(TERM_TAIL_COUNTER_OFFSET);
        verify(gapHandler).onGap(termBuffer, 0, frameOffset);
    }

    @Test
    public void shouldReportSingleGapWhenBufferNotFull()
    {
        final int tail = FRAME_ALIGNMENT;
        final int highWaterMark = FRAME_ALIGNMENT * 3;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET)).thenReturn(tail);
        when(metaDataBuffer.getIntVolatile(TERM_HIGH_WATER_MARK_OFFSET)).thenReturn(highWaterMark);

        when(termBuffer.getIntVolatile(lengthOffset(tail - FRAME_ALIGNMENT)))
            .thenReturn(FRAME_ALIGNMENT);
        when(termBuffer.getIntVolatile(lengthOffset(tail)))
            .thenReturn(0);
        when(termBuffer.getIntVolatile(lengthOffset(highWaterMark - FRAME_ALIGNMENT)))
            .thenReturn(FRAME_ALIGNMENT);

        assertThat(scanner.scan(gapHandler), is(1));

        final InOrder inOrder = inOrder(metaDataBuffer);
        inOrder.verify(metaDataBuffer).getIntVolatile(TERM_HIGH_WATER_MARK_OFFSET);
        inOrder.verify(metaDataBuffer).getIntVolatile(TERM_TAIL_COUNTER_OFFSET);
        verify(gapHandler).onGap(termBuffer, tail, FRAME_ALIGNMENT);
    }

    @Test
    public void shouldReportSingleGapWhenBufferIsFull()
    {
        final int tail = LOG_BUFFER_CAPACITY - (FRAME_ALIGNMENT * 2);
        final int highWaterMark = LOG_BUFFER_CAPACITY;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET)).thenReturn(tail);
        when(metaDataBuffer.getIntVolatile(TERM_HIGH_WATER_MARK_OFFSET)).thenReturn(highWaterMark);

        when(termBuffer.getIntVolatile(lengthOffset(tail - FRAME_ALIGNMENT)))
            .thenReturn(FRAME_ALIGNMENT);
        when(termBuffer.getIntVolatile(lengthOffset(tail)))
            .thenReturn(0);
        when(termBuffer.getIntVolatile(lengthOffset(highWaterMark - FRAME_ALIGNMENT)))
            .thenReturn(FRAME_ALIGNMENT);

        assertThat(scanner.scan(gapHandler), is(1));

        final InOrder inOrder = inOrder(metaDataBuffer);
        inOrder.verify(metaDataBuffer).getIntVolatile(TERM_HIGH_WATER_MARK_OFFSET);
        inOrder.verify(metaDataBuffer).getIntVolatile(TERM_TAIL_COUNTER_OFFSET);
        verify(gapHandler).onGap(termBuffer, tail, FRAME_ALIGNMENT);
    }

    @Test
    public void shouldReportMultipleGaps()
    {
        final int tail = FRAME_ALIGNMENT;
        final int highWaterMark = FRAME_ALIGNMENT * 6;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET)).thenReturn(tail);
        when(metaDataBuffer.getIntVolatile(TERM_HIGH_WATER_MARK_OFFSET)).thenReturn(highWaterMark);

        when(termBuffer.getIntVolatile(lengthOffset(0)))
            .thenReturn(FRAME_ALIGNMENT);
        when(termBuffer.getIntVolatile(lengthOffset(FRAME_ALIGNMENT)))
            .thenReturn(0);
        when(termBuffer.getIntVolatile(lengthOffset(FRAME_ALIGNMENT * 2)))
            .thenReturn(FRAME_ALIGNMENT);
        when(termBuffer.getIntVolatile(lengthOffset(FRAME_ALIGNMENT * 3)))
            .thenReturn(0);
        when(termBuffer.getIntVolatile(lengthOffset(FRAME_ALIGNMENT * 5)))
            .thenReturn(FRAME_ALIGNMENT);
        when(gapHandler.onGap(termBuffer, FRAME_ALIGNMENT, FRAME_ALIGNMENT)).thenReturn(true);

        assertThat(scanner.scan(gapHandler), is(2));

        final InOrder inOrder = inOrder(metaDataBuffer);
        inOrder.verify(metaDataBuffer).getIntVolatile(TERM_HIGH_WATER_MARK_OFFSET);
        inOrder.verify(metaDataBuffer).getIntVolatile(TERM_TAIL_COUNTER_OFFSET);
        verify(gapHandler).onGap(termBuffer, FRAME_ALIGNMENT, FRAME_ALIGNMENT);
        verify(gapHandler).onGap(termBuffer, FRAME_ALIGNMENT * 3, FRAME_ALIGNMENT * 2);
    }
}

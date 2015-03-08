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
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.lengthOffset;

public class TermGapScannerTest
{
    private static final int LOG_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int TERM_ID = 1;

    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);
    private final TermGapScanner.GapHandler gapHandler = mock(TermGapScanner.GapHandler.class);

    @Before
    public void setUp()
    {
        when(termBuffer.capacity()).thenReturn(LOG_BUFFER_CAPACITY);
    }

    @Test
    public void shouldReportNoGapsOnEmptyBuffer()
    {
        assertThat(TermGapScanner.scanForGaps(termBuffer, TERM_ID, 0, 0, gapHandler), is(0));

        verifyZeroInteractions(gapHandler);
    }

    @Test
    public void shouldReportNoGapsWhenTailEqualsHighWaterMark()
    {
        final int fillLevel = FRAME_ALIGNMENT * 4;

        assertThat(TermGapScanner.scanForGaps(termBuffer, TERM_ID, fillLevel, fillLevel, gapHandler), is(0));

        verifyZeroInteractions(gapHandler);
    }

    @Test
    public void shouldReportGapAtBeginningOfBuffer()
    {
        final int frameOffset = FRAME_ALIGNMENT * 3;
        final int highWaterMark = frameOffset + FRAME_ALIGNMENT;

        when(termBuffer.getInt(lengthOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);

        assertThat(TermGapScanner.scanForGaps(termBuffer, TERM_ID, 0, highWaterMark, gapHandler), is(1));

        verify(gapHandler).onGap(TERM_ID, termBuffer, 0, frameOffset);
    }

    @Test
    public void shouldReportSingleGapWhenBufferNotFull()
    {
        final int tail = FRAME_ALIGNMENT;
        final int highWaterMark = FRAME_ALIGNMENT * 3;

        when(termBuffer.getInt(lengthOffset(tail - FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);
        when(termBuffer.getInt(lengthOffset(tail), LITTLE_ENDIAN))
            .thenReturn(0);
        when(termBuffer.getInt(lengthOffset(highWaterMark - FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);

        assertThat(TermGapScanner.scanForGaps(termBuffer, TERM_ID, tail, highWaterMark, gapHandler), is(1));

        verify(gapHandler).onGap(TERM_ID, termBuffer, tail, FRAME_ALIGNMENT);
    }

    @Test
    public void shouldReportSingleGapWhenBufferIsFull()
    {
        final int tail = LOG_BUFFER_CAPACITY - (FRAME_ALIGNMENT * 2);
        final int highWaterMark = LOG_BUFFER_CAPACITY;

        when(termBuffer.getInt(lengthOffset(tail - FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);
        when(termBuffer.getInt(lengthOffset(tail), LITTLE_ENDIAN))
            .thenReturn(0);
        when(termBuffer.getInt(lengthOffset(highWaterMark - FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);

        assertThat(TermGapScanner.scanForGaps(termBuffer, TERM_ID, tail, highWaterMark, gapHandler), is(1));

        verify(gapHandler).onGap(TERM_ID, termBuffer, tail, FRAME_ALIGNMENT);
    }

    @Test
    public void shouldReportMultipleGaps()
    {
        final int tail = FRAME_ALIGNMENT;
        final int highWaterMark = FRAME_ALIGNMENT * 6;

        when(termBuffer.getInt(lengthOffset(0), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);
        when(termBuffer.getInt(lengthOffset(FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn(0);
        when(termBuffer.getInt(lengthOffset(FRAME_ALIGNMENT * 2), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);
        when(termBuffer.getInt(lengthOffset(FRAME_ALIGNMENT * 3), LITTLE_ENDIAN))
            .thenReturn(0);
        when(termBuffer.getInt(lengthOffset(FRAME_ALIGNMENT * 5), LITTLE_ENDIAN))
            .thenReturn(FRAME_ALIGNMENT);

        when(gapHandler.onGap(TERM_ID, termBuffer, FRAME_ALIGNMENT, FRAME_ALIGNMENT))
            .thenReturn(true);

        assertThat(TermGapScanner.scanForGaps(termBuffer, TERM_ID, tail, highWaterMark, gapHandler), is(2));

        final InOrder inOrder = inOrder(gapHandler);
        inOrder.verify(gapHandler).onGap(TERM_ID, termBuffer, FRAME_ALIGNMENT, FRAME_ALIGNMENT);
        inOrder.verify(gapHandler).onGap(TERM_ID, termBuffer, FRAME_ALIGNMENT * 3, FRAME_ALIGNMENT * 2);
    }
}

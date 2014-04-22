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

import static java.lang.Integer.valueOf;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.BufferDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.*;

public class MtuScannerTest
{
    private static final int LOG_BUFFER_CAPACITY = 1024 * 16;
    private static final int STATE_BUFFER_CAPACITY = STATE_BUFFER_LENGTH;
    private static final int MTU_LENGTH = 1024;
    private static final int HEADER_LENGTH = 32;
    private static final short MSG_TYPE = 7;

    private final AtomicBuffer logBuffer = mock(AtomicBuffer.class);
    private final AtomicBuffer stateBuffer = mock(AtomicBuffer.class);

    private MtuScanner scanner;

    @Before
    public void setUp()
    {
        when(logBuffer.capacity()).thenReturn(LOG_BUFFER_CAPACITY);
        when(stateBuffer.capacity()).thenReturn(STATE_BUFFER_CAPACITY);

        scanner = new MtuScanner(logBuffer, stateBuffer, MTU_LENGTH, HEADER_LENGTH);
    }

    @Test
    public void shouldReportUnderlyingCapacity()
    {
        assertThat(scanner.capacity(), is(LOG_BUFFER_CAPACITY));
    }

    @Test
    public void shouldReportMtu()
    {
        assertThat(scanner.mtuLength(), is(MTU_LENGTH));
    }

    @Test
    public void shouldReturnFalseOnEmptyLog()
    {
        assertFalse(scanner.scanNext());
    }

    @Test
    public void shouldScanSingleMessage()
    {
        final int frameOffset = 0;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn(MSG_TYPE);

        assertTrue(scanner.scanNext());
        assertThat(scanner.offset(), is(frameOffset));
        assertThat(scanner.length(), is(FRAME_ALIGNMENT));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);
    }

    @Test
    public void shouldScanTwoMessagesThatFitInSingleMtu()
    {
        int frameOffset = 0;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(FRAME_ALIGNMENT * 2);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn(MSG_TYPE);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + FRAME_ALIGNMENT)))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getShort(typeOffset(frameOffset + FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn(MSG_TYPE);

        assertTrue(scanner.scanNext());
        assertThat(scanner.offset(), is(frameOffset));
        assertThat(scanner.length(), is(FRAME_ALIGNMENT * 2));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);

        frameOffset += FRAME_ALIGNMENT;
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);
    }

    @Test
    public void shouldScanTwoMessagesAndStopAtMtuBoundary()
    {
        final int frameOneLength = MTU_LENGTH - FRAME_ALIGNMENT;
        final int frameTwoLength = FRAME_ALIGNMENT;
        int frameOffset = 0;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(frameOneLength + frameTwoLength);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameOneLength);
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn(MSG_TYPE);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + frameOneLength)))
            .thenReturn(frameTwoLength);
        when(logBuffer.getShort(typeOffset(frameOffset + frameOneLength), LITTLE_ENDIAN))
            .thenReturn(MSG_TYPE);

        assertTrue(scanner.scanNext());
        assertThat(scanner.offset(), is(frameOffset));
        assertThat(scanner.length(), is(frameOneLength + frameTwoLength));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);

        frameOffset += frameOneLength;
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);
    }

    @Test
    public void shouldScanTwoMessagesAndStopAtSecondThatSpansMtu()
    {
        final int frameOneLength = MTU_LENGTH - FRAME_ALIGNMENT;
        final int frameTwoLength = FRAME_ALIGNMENT * 2;
        int frameOffset = 0;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(frameOneLength + frameTwoLength);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameOneLength);
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn(MSG_TYPE);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + frameOneLength)))
            .thenReturn(frameTwoLength);
        when(logBuffer.getShort(typeOffset(frameOffset + frameOneLength), LITTLE_ENDIAN))
            .thenReturn(MSG_TYPE);

        assertTrue(scanner.scanNext());
        assertThat(scanner.offset(), is(frameOffset));
        assertThat(scanner.length(), is(frameOneLength));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);

        frameOffset += frameOneLength;
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);
    }

    @Test
    public void shouldScanLastFrameInBuffer()
    {
        final int frameOffset = LOG_BUFFER_CAPACITY - FRAME_ALIGNMENT;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(LOG_BUFFER_CAPACITY);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn(MSG_TYPE);

        scanner.seek(frameOffset);

        assertTrue(scanner.scanNext());
        assertThat(scanner.offset(), is(frameOffset));
        assertThat(scanner.length(), is(FRAME_ALIGNMENT));
        assertTrue(scanner.isComplete());
        assertFalse(scanner.scanNext());
    }

    @Test
    public void shouldScanLastMessageInBufferPlusPadding()
    {
        final int frameOffset = LOG_BUFFER_CAPACITY - (FRAME_ALIGNMENT * 3);

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(LOG_BUFFER_CAPACITY - FRAME_ALIGNMENT);
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset))))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn(MSG_TYPE);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + FRAME_ALIGNMENT)))
            .thenReturn(FRAME_ALIGNMENT * 2);
        when(logBuffer.getShort(typeOffset(frameOffset + FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn(PADDING_MSG_TYPE);

        scanner.seek(frameOffset);

        assertTrue(scanner.scanNext());
        assertThat(scanner.offset(), is(frameOffset));
        assertThat(scanner.length(), is(FRAME_ALIGNMENT * 2));
        assertTrue(scanner.isComplete());
        assertFalse(scanner.scanNext());
    }
}

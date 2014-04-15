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
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.PADDING_MSG_TYPE;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.TAIL_COUNTER_OFFSET;

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
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(LOG_BUFFER_CAPACITY));
        when(valueOf(stateBuffer.capacity())).thenReturn(valueOf(STATE_BUFFER_CAPACITY));

        scanner = new MtuScanner(logBuffer, stateBuffer, MTU_LENGTH, HEADER_LENGTH);
    }

    @Test
    public void shouldReportUnderlyingCapacity()
    {
        assertThat(valueOf(scanner.capacity()), is(valueOf(LOG_BUFFER_CAPACITY)));
    }

    @Test
    public void shouldReportMtu()
    {
        assertThat(valueOf(scanner.mtuLength()), is(valueOf(MTU_LENGTH)));
    }

    @Test
    public void shouldReturnFalseOnEmptyLog()
    {
        assertFalse(scanner.scan());
    }

    @Test
    public void shouldScanSingleMessage()
    {
        final int frameOffset = 0;

        when(valueOf(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)))
            .thenReturn(valueOf(FRAME_ALIGNMENT));
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset))))
            .thenReturn(valueOf(FRAME_ALIGNMENT));
        when(Short.valueOf(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN)))
            .thenReturn(Short.valueOf(MSG_TYPE));

        assertTrue(scanner.scan());
        assertThat(valueOf(scanner.offset()), is(valueOf(frameOffset)));
        assertThat(valueOf(scanner.length()), is(valueOf(FRAME_ALIGNMENT)));
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

        when(valueOf(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)))
            .thenReturn(valueOf(FRAME_ALIGNMENT * 2));
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset))))
            .thenReturn(valueOf(FRAME_ALIGNMENT));
        when(Short.valueOf(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN)))
            .thenReturn(Short.valueOf(MSG_TYPE));
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset + FRAME_ALIGNMENT))))
            .thenReturn(valueOf(FRAME_ALIGNMENT));
        when(Short.valueOf(logBuffer.getShort(typeOffset(frameOffset + FRAME_ALIGNMENT), LITTLE_ENDIAN)))
            .thenReturn(Short.valueOf(MSG_TYPE));

        assertTrue(scanner.scan());
        assertThat(valueOf(scanner.offset()), is(valueOf(frameOffset)));
        assertThat(valueOf(scanner.length()), is(valueOf(FRAME_ALIGNMENT * 2)));
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

        when(valueOf(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)))
            .thenReturn(valueOf(frameOneLength + frameTwoLength));
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset))))
            .thenReturn(valueOf(frameOneLength));
        when(Short.valueOf(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN)))
            .thenReturn(Short.valueOf(MSG_TYPE));
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset + frameOneLength))))
            .thenReturn(valueOf(frameTwoLength));
        when(Short.valueOf(logBuffer.getShort(typeOffset(frameOffset + frameOneLength), LITTLE_ENDIAN)))
            .thenReturn(Short.valueOf(MSG_TYPE));

        assertTrue(scanner.scan());
        assertThat(valueOf(scanner.offset()), is(valueOf(frameOffset)));
        assertThat(valueOf(scanner.length()), is(valueOf(frameOneLength + frameTwoLength)));
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

        when(valueOf(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)))
            .thenReturn(valueOf(frameOneLength + frameTwoLength));
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset))))
            .thenReturn(valueOf(frameOneLength));
        when(Short.valueOf(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN)))
            .thenReturn(Short.valueOf(MSG_TYPE));
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset + frameOneLength))))
            .thenReturn(valueOf(frameTwoLength));
        when(Short.valueOf(logBuffer.getShort(typeOffset(frameOffset + frameOneLength), LITTLE_ENDIAN)))
            .thenReturn(Short.valueOf(MSG_TYPE));

        assertTrue(scanner.scan());
        assertThat(valueOf(scanner.offset()), is(valueOf(frameOffset)));
        assertThat(valueOf(scanner.length()), is(valueOf(frameOneLength)));
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

        when(valueOf(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)))
            .thenReturn(valueOf(LOG_BUFFER_CAPACITY));
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset))))
            .thenReturn(valueOf(FRAME_ALIGNMENT));
        when(Short.valueOf(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN)))
            .thenReturn(Short.valueOf(MSG_TYPE));

        scanner.seek(frameOffset);
        assertTrue(scanner.scan());
        assertThat(valueOf(scanner.offset()), is(valueOf(frameOffset)));
        assertThat(valueOf(scanner.length()), is(valueOf(FRAME_ALIGNMENT)));
        assertTrue(scanner.isComplete());
        assertFalse(scanner.scan());
    }

    @Test
    public void shouldScanLastMessageInBufferPlusPadding()
    {
        final int frameOffset = LOG_BUFFER_CAPACITY - (FRAME_ALIGNMENT * 3);

        when(valueOf(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)))
            .thenReturn(valueOf(LOG_BUFFER_CAPACITY - FRAME_ALIGNMENT));
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset))))
            .thenReturn(valueOf(FRAME_ALIGNMENT));
        when(Short.valueOf(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN)))
            .thenReturn(Short.valueOf(MSG_TYPE));
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset + FRAME_ALIGNMENT))))
            .thenReturn(valueOf(FRAME_ALIGNMENT * 2));
        when(Short.valueOf(logBuffer.getShort(typeOffset(frameOffset + FRAME_ALIGNMENT), LITTLE_ENDIAN)))
            .thenReturn(Short.valueOf(PADDING_MSG_TYPE));

        scanner.seek(frameOffset);
        assertTrue(scanner.scan());
        assertThat(valueOf(scanner.offset()), is(valueOf(frameOffset)));
        assertThat(valueOf(scanner.length()), is(valueOf(FRAME_ALIGNMENT * 2)));
        assertTrue(scanner.isComplete());
        assertFalse(scanner.scan());
    }
}

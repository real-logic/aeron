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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.BitUtil.align;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.TAIL_COUNTER_OFFSET;

public class LogBufferAppenderTest
{
    private static final int LOG_BUFFER_SIZE = 1024 * 16;
    private static final int STATE_BUFFER_SIZE = 1024 * 4;
    private static final int HEADER_RESERVE = 24;

    private final AtomicBuffer logBuffer = mock(AtomicBuffer.class);
    private final AtomicBuffer stateBuffer = mock(AtomicBuffer.class);

    private LogBufferAppender appender;

    @Before
    public void setUp()
    {
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(LOG_BUFFER_SIZE));
        when(valueOf(stateBuffer.capacity())).thenReturn(valueOf(STATE_BUFFER_SIZE));

        appender = new LogBufferAppender(logBuffer, stateBuffer, HEADER_RESERVE);
    }

    @Test
    public void shouldReportCapacity()
    {
        assertThat(valueOf(appender.capacity()), is(valueOf(LOG_BUFFER_SIZE)));
    }

    @Test
    public void shouldReportHeaderReserveLength()
    {
        assertThat(valueOf(appender.headerReserveLength()), is(valueOf(HEADER_RESERVE)));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientCapacityForLog()
    {
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(LogBufferDescriptor.LOG_MIN_SIZE - 1));

        appender = new LogBufferAppender(logBuffer, stateBuffer, HEADER_RESERVE);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCapacityNotMultipleOfAlignment()
    {
        final int logBufferCapacity = LogBufferDescriptor.LOG_MIN_SIZE + FRAME_ALIGNMENT + 1;
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(logBufferCapacity));

        appender = new LogBufferAppender(logBuffer, stateBuffer, HEADER_RESERVE);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientStateBufferCapacity()
    {
        when(valueOf(stateBuffer.capacity())).thenReturn(valueOf(LogBufferDescriptor.STATE_SIZE - 1));

        appender = new LogBufferAppender(logBuffer, stateBuffer, HEADER_RESERVE);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnHeaderReserveNotOnWordSizeBoundary()
    {
        appender = new LogBufferAppender(logBuffer, stateBuffer, 3);
    }

    @Test
    public void shouldReportCurrentTail()
    {
        final int tailValue = 64;
        when(valueOf(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))).thenReturn(valueOf(tailValue));

        assertThat(valueOf(appender.tail()), is(valueOf(tailValue)));
    }

    @Test
    public void shouldReportCurrentTailAtCapacity()
    {
        final int tailValue = LOG_BUFFER_SIZE + 64;
        when(valueOf(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))).thenReturn(valueOf(tailValue));

        assertThat(valueOf(appender.tail()), is(valueOf(LOG_BUFFER_SIZE)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenMaxMessageLengthExceeded()
    {
        final int maxMessageLength = appender.maxMessageLength();
        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);

        appender.append(srcBuffer, 0, maxMessageLength + 1);
    }

    @Test
    public void shouldAppendFrameToEmptyLog()
    {
        when(valueOf(stateBuffer.getAndAddInt(TAIL_COUNTER_OFFSET, FRAME_ALIGNMENT)))
            .thenReturn(valueOf(0));

        final AtomicBuffer buffer = new AtomicBuffer(new byte[128]);
        final int msgLen = 20;

        assertTrue(appender.append(buffer, 0, msgLen));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, FRAME_ALIGNMENT);
        inOrder.verify(logBuffer, times(1)).putBytes(messageOffset(0, HEADER_RESERVE), buffer, 0, msgLen);
        inOrder.verify(logBuffer, times(1)).putIntOrdered(lengthOffset(0), FRAME_ALIGNMENT);
    }

    @Test
    public void shouldAppendFrameTwiceToLog()
    {
        when(valueOf(stateBuffer.getAndAddInt(TAIL_COUNTER_OFFSET, FRAME_ALIGNMENT)))
            .thenReturn(valueOf(0))
            .thenReturn(valueOf(FRAME_ALIGNMENT));

        final AtomicBuffer buffer = new AtomicBuffer(new byte[128]);
        final int msgLen = 20;

        assertTrue(appender.append(buffer, 0, msgLen));
        assertTrue(appender.append(buffer, 0, msgLen));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, FRAME_ALIGNMENT);
        inOrder.verify(logBuffer, times(1)).putBytes(messageOffset(0, HEADER_RESERVE), buffer, 0, msgLen);
        inOrder.verify(logBuffer, times(1)).putIntOrdered(lengthOffset(0), FRAME_ALIGNMENT);
        inOrder.verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, FRAME_ALIGNMENT);
        inOrder.verify(logBuffer, times(1)).putBytes(messageOffset(FRAME_ALIGNMENT, HEADER_RESERVE), buffer, 0, msgLen);
        inOrder.verify(logBuffer, times(1)).putIntOrdered(lengthOffset(FRAME_ALIGNMENT), FRAME_ALIGNMENT);
    }

    @Test
    public void shouldFailToAppendToLogAtCapacity()
    {
        when(valueOf(stateBuffer.getAndAddInt(TAIL_COUNTER_OFFSET, FRAME_ALIGNMENT)))
            .thenReturn(valueOf(appender.capacity()));

        final AtomicBuffer buffer = new AtomicBuffer(new byte[128]);
        final int msgLength = 20;

        assertFalse(appender.append(buffer, 0, msgLength));

        verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, FRAME_ALIGNMENT);
        verify(logBuffer, never()).putBytes(anyInt(), eq(buffer), anyInt(), anyInt());
        verify(logBuffer, never()).putIntOrdered(anyInt(), anyInt());
    }

    @Test
    public void shouldPadLogAndFailAppendOnInsufficientRemainingCapacity()
    {
        final int msgLength = 120;
        final int requiredFrameSize = align(FIXED_HEADER_LENGTH + HEADER_RESERVE + msgLength, FRAME_ALIGNMENT);
        final int tailValue = appender.capacity() - FRAME_ALIGNMENT;
        when(valueOf(stateBuffer.getAndAddInt(TAIL_COUNTER_OFFSET, requiredFrameSize)))
            .thenReturn(valueOf(tailValue));

        final AtomicBuffer buffer = new AtomicBuffer(new byte[128]);

        assertFalse(appender.append(buffer, 0, msgLength));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, requiredFrameSize);
        inOrder.verify(logBuffer, times(1)).putIntOrdered(tailValue + LENGTH_FIELD_OFFSET, FRAME_ALIGNMENT);

        verify(logBuffer, never()).putBytes(anyInt(), eq(buffer), anyInt(), anyInt());
    }
}

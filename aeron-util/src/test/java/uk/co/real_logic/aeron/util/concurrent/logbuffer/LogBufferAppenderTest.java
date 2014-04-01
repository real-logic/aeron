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
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.TAIL_COUNTER_OFFSET;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.RecordDescriptor.RECORD_ALIGNMENT;

public class LogBufferAppenderTest
{
    private static final int LOG_BUFFER_SIZE = 1024 * 16;
    private static final int STATE_BUFFER_SIZE = 1024 * 4;
    private static final int RECORD_HEADER_LENGTH = 24;
    private static final int LENGTH_FIELD_OFFSET = 4;

    private final AtomicBuffer logBuffer = mock(AtomicBuffer.class);
    private final AtomicBuffer stateBuffer = mock(AtomicBuffer.class);

    private LogBufferAppender appender;

    @Before
    public void setUp()
    {
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(LOG_BUFFER_SIZE));
        when(valueOf(stateBuffer.capacity())).thenReturn(valueOf(STATE_BUFFER_SIZE));

        appender = new LogBufferAppender(logBuffer, stateBuffer, RECORD_HEADER_LENGTH, LENGTH_FIELD_OFFSET);
    }

    @Test
    public void shouldReportCapacity()
    {
        assertThat(valueOf(appender.capacity()), is(valueOf(LOG_BUFFER_SIZE)));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientCapacityForLog()
    {
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(LogBufferDescriptor.LOG_MIN_SIZE - 1));

        appender = new LogBufferAppender(logBuffer, stateBuffer, RECORD_HEADER_LENGTH, LENGTH_FIELD_OFFSET);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCapacityNotMultipleOfAlignment()
    {
        final int logBufferCapacity = LogBufferDescriptor.LOG_MIN_SIZE + RECORD_ALIGNMENT + 1;
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(logBufferCapacity));

        appender = new LogBufferAppender(logBuffer, stateBuffer, RECORD_HEADER_LENGTH, LENGTH_FIELD_OFFSET);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientStateBufferCapacity()
    {
        when(valueOf(stateBuffer.capacity())).thenReturn(valueOf(LogBufferDescriptor.STATE_SIZE - 1));

        appender = new LogBufferAppender(logBuffer, stateBuffer, RECORD_HEADER_LENGTH, LENGTH_FIELD_OFFSET);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnRecordSizeBeingLessThanWordSize()
    {
        appender = new LogBufferAppender(logBuffer, stateBuffer, 7, LENGTH_FIELD_OFFSET);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnRecordSizeNotBeingMultipleOfWordSize()
    {
        appender = new LogBufferAppender(logBuffer, stateBuffer, 17, LENGTH_FIELD_OFFSET);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void shouldThrowExceptionOnRecordLengthFieldOffsetNotInHeaderLengthRange()
    {
        appender = new LogBufferAppender(logBuffer, stateBuffer, RECORD_HEADER_LENGTH, RECORD_HEADER_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnRecordLengthFieldOffsetNotOnIntSizeBoundary()
    {
        appender = new LogBufferAppender(logBuffer, stateBuffer, RECORD_HEADER_LENGTH, 3);
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
    public void shouldAppendRecordToEmptyLog()
    {
        when(valueOf(stateBuffer.getAndAddInt(TAIL_COUNTER_OFFSET, RECORD_ALIGNMENT)))
            .thenReturn(valueOf(0));

        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[128]);
        final int msgLength = 20;

        assertTrue(appender.append(srcBuffer, 0, msgLength));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, RECORD_ALIGNMENT);
        inOrder.verify(logBuffer, times(1)).putBytes(RECORD_HEADER_LENGTH, srcBuffer, 0, msgLength);
        inOrder.verify(logBuffer, times(1)).putIntOrdered(LENGTH_FIELD_OFFSET, RECORD_ALIGNMENT);
    }

    @Test
    public void shouldAppendRecordTwiceToLog()
    {
        when(valueOf(stateBuffer.getAndAddInt(TAIL_COUNTER_OFFSET, RECORD_ALIGNMENT)))
            .thenReturn(valueOf(0))
            .thenReturn(valueOf(RECORD_ALIGNMENT));

        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[128]);
        final int msgLength = 20;

        assertTrue(appender.append(srcBuffer, 0, msgLength));
        assertTrue(appender.append(srcBuffer, 0, msgLength));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, RECORD_ALIGNMENT);
        inOrder.verify(logBuffer, times(1)).putBytes(RECORD_HEADER_LENGTH, srcBuffer, 0, msgLength);
        inOrder.verify(logBuffer, times(1)).putIntOrdered(LENGTH_FIELD_OFFSET, RECORD_ALIGNMENT);
        inOrder.verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, RECORD_ALIGNMENT);
        inOrder.verify(logBuffer, times(1)).putBytes(RECORD_ALIGNMENT + RECORD_HEADER_LENGTH, srcBuffer, 0, msgLength);
        inOrder.verify(logBuffer, times(1)).putIntOrdered(RECORD_ALIGNMENT + LENGTH_FIELD_OFFSET, RECORD_ALIGNMENT);
    }

    @Test
    public void shouldFailToAppendToLogAtCapacity()
    {
        when(valueOf(stateBuffer.getAndAddInt(TAIL_COUNTER_OFFSET, RECORD_ALIGNMENT)))
            .thenReturn(valueOf(appender.capacity()));

        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[128]);
        final int msgLength = 20;

        assertFalse(appender.append(srcBuffer, 0, msgLength));

        verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, RECORD_ALIGNMENT);
        verify(logBuffer, never()).putBytes(anyInt(), eq(srcBuffer), anyInt(), anyInt());
        verify(logBuffer, never()).putIntOrdered(anyInt(), anyInt());
    }

    @Test
    public void shouldPadLogAndFailAppendOnInsufficientRemainingCapacity()
    {
        final int msgLength = 120;
        final int requiredRecordSize = align(RECORD_HEADER_LENGTH + msgLength, RECORD_ALIGNMENT);
        final int tailValue = appender.capacity() - RECORD_ALIGNMENT;
        when(valueOf(stateBuffer.getAndAddInt(TAIL_COUNTER_OFFSET, requiredRecordSize)))
            .thenReturn(valueOf(tailValue));

        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[128]);

        assertFalse(appender.append(srcBuffer, 0, msgLength));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, requiredRecordSize);
        inOrder.verify(logBuffer, times(1)).putIntOrdered(tailValue + LENGTH_FIELD_OFFSET, RECORD_ALIGNMENT);

        verify(logBuffer, never()).putBytes(anyInt(), eq(srcBuffer), anyInt(), anyInt());
    }
}

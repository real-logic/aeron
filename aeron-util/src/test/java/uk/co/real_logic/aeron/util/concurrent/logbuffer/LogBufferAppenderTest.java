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
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.BitUtil.align;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.BaseMessageHeaderFlyweight.BASE_HEADER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.TAIL_COUNTER_OFFSET;

public class LogBufferAppenderTest
{
    private static final int LOG_BUFFER_SIZE = 1024 * 16;
    private static final int STATE_BUFFER_SIZE = 1024;
    private static final int MAX_FRAME_LENGTH = 1024;
    private static final byte[] DEFAULT_HEADER = new byte[BASE_HEADER_LENGTH + SIZE_OF_INT];

    private final AtomicBuffer logBuffer = mock(AtomicBuffer.class);
    private final AtomicBuffer stateBuffer = mock(AtomicBuffer.class);
    private final BaseMessageHeaderFlyweight header = mock(BaseMessageHeaderFlyweight.class);

    private LogBufferAppender appender;

    @Before
    public void setUp()
    {
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(LOG_BUFFER_SIZE));
        when(valueOf(stateBuffer.capacity())).thenReturn(valueOf(STATE_BUFFER_SIZE));

        when(header.wrap(eq(logBuffer), anyInt())).thenReturn(header);
        when(header.beginFragment(anyBoolean())).thenReturn(header);
        when(header.endFragment(anyBoolean())).thenReturn(header);
        when(header.sequenceNumber(anyInt())).thenReturn(header);
        when(header.putLengthOrdered(anyInt())).thenReturn(header);

        appender = new LogBufferAppender(logBuffer, stateBuffer, header, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test
    public void shouldReportCapacity()
    {
        assertThat(valueOf(appender.capacity()), is(valueOf(LOG_BUFFER_SIZE)));
    }

    @Test
    public void shouldReportMaxFrameLength()
    {
        assertThat(valueOf(appender.maxFrameLength()), is(valueOf(MAX_FRAME_LENGTH)));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientCapacityForLog()
    {
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(LogBufferDescriptor.LOG_MIN_SIZE - 1));

        appender = new LogBufferAppender(logBuffer, stateBuffer, header, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCapacityNotMultipleOfAlignment()
    {
        final int logBufferCapacity = LogBufferDescriptor.LOG_MIN_SIZE + FRAME_ALIGNMENT + 1;
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(logBufferCapacity));

        appender = new LogBufferAppender(logBuffer, stateBuffer, header, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientStateBufferCapacity()
    {
        when(valueOf(stateBuffer.capacity())).thenReturn(valueOf(LogBufferDescriptor.STATE_SIZE - 1));

        appender = new LogBufferAppender(logBuffer, stateBuffer, header, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnDefaultHeaderLengthLessThanBaseHeaderLength()
    {
        int length = BaseMessageHeaderFlyweight.BASE_HEADER_LENGTH - 1;
        appender = new LogBufferAppender(logBuffer, stateBuffer, header, new byte[length], MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnDefaultHeaderLengthNotOnWordSizeBoundary()
    {
        appender = new LogBufferAppender(logBuffer, stateBuffer, header, new byte[31], MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnMaxFrameSizeNotOnWordSizeBoundary()
    {
        appender = new LogBufferAppender(logBuffer, stateBuffer, header, DEFAULT_HEADER, 1001);
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

        final InOrder inOrder = inOrder(logBuffer, stateBuffer, header);
        inOrder.verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, FRAME_ALIGNMENT);
        inOrder.verify(logBuffer, times(1)).putBytes(0, DEFAULT_HEADER, 0, DEFAULT_HEADER.length);
        inOrder.verify(logBuffer, times(1)).putBytes(DEFAULT_HEADER.length, buffer, 0, msgLen);
        inOrder.verify(header, times(1)).beginFragment(true);
        inOrder.verify(header, times(1)).endFragment(true);
        inOrder.verify(header, times(1)).sequenceNumber(0);
        inOrder.verify(header, times(1)).putLengthOrdered(FRAME_ALIGNMENT);
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

        final InOrder inOrder = inOrder(logBuffer, stateBuffer, header);
        inOrder.verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, FRAME_ALIGNMENT);
        inOrder.verify(logBuffer, times(1)).putBytes(0, DEFAULT_HEADER, 0, DEFAULT_HEADER.length);
        inOrder.verify(logBuffer, times(1)).putBytes(DEFAULT_HEADER.length, buffer, 0, msgLen);
        inOrder.verify(header, times(1)).beginFragment(true);
        inOrder.verify(header, times(1)).endFragment(true);
        inOrder.verify(header, times(1)).sequenceNumber(0);
        inOrder.verify(header, times(1)).putLengthOrdered(FRAME_ALIGNMENT);

        inOrder.verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, FRAME_ALIGNMENT);
        inOrder.verify(logBuffer, times(1)).putBytes(FRAME_ALIGNMENT, DEFAULT_HEADER, 0, DEFAULT_HEADER.length);
        inOrder.verify(logBuffer, times(1)).putBytes(FRAME_ALIGNMENT + DEFAULT_HEADER.length, buffer, 0, msgLen);
        inOrder.verify(header, times(1)).beginFragment(true);
        inOrder.verify(header, times(1)).endFragment(true);
        inOrder.verify(header, times(1)).sequenceNumber(FRAME_ALIGNMENT);
        inOrder.verify(header, times(1)).putLengthOrdered(FRAME_ALIGNMENT);
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
        verify(header, never()).sequenceNumber(anyInt());
        verify(header, never()).putLengthOrdered(anyInt());
    }

    @Test
    public void shouldPadLogAndFailAppendOnInsufficientRemainingCapacity()
    {
        final int msgLength = 120;
        final int requiredFrameSize = align(DEFAULT_HEADER.length + msgLength, FRAME_ALIGNMENT);
        final int tailValue = appender.capacity() - FRAME_ALIGNMENT;
        when(valueOf(stateBuffer.getAndAddInt(TAIL_COUNTER_OFFSET, requiredFrameSize)))
            .thenReturn(valueOf(tailValue));

        final AtomicBuffer buffer = new AtomicBuffer(new byte[128]);

        assertFalse(appender.append(buffer, 0, msgLength));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer, header);
        inOrder.verify(stateBuffer, times(1)).getAndAddInt(TAIL_COUNTER_OFFSET, requiredFrameSize);
        inOrder.verify(logBuffer, times(1)).putBytes(tailValue, DEFAULT_HEADER, 0, DEFAULT_HEADER.length);
        inOrder.verify(header, times(1)).beginFragment(true);
        inOrder.verify(header, times(1)).endFragment(true);
        inOrder.verify(header, times(1)).sequenceNumber(tailValue);
        inOrder.verify(header, times(1)).putLengthOrdered(FRAME_ALIGNMENT);
    }
}

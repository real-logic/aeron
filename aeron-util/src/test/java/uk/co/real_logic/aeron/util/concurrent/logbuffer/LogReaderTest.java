/*
 * Copyright 2013 Real Logic Ltd.
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
import org.mockito.Mockito;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.BitUtil.align;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.util.protocol.HeaderFlyweight.HDR_TYPE_DATA;

public class LogReaderTest
{
    private static final int LOG_BUFFER_CAPACITY = 1024 * 16;
    private static final int STATE_BUFFER_CAPACITY = STATE_BUFFER_LENGTH;
    private static final int HEADER_LENGTH = BASE_HEADER_LENGTH;

    private final AtomicBuffer logBuffer = mock(AtomicBuffer.class);
    private final AtomicBuffer stateBuffer = spy(new AtomicBuffer(new byte[STATE_BUFFER_CAPACITY]));
    private final LogReader.FrameHandler handler = Mockito.mock(LogReader.FrameHandler.class);

    private LogReader logReader;

    @Before
    public void setUp()
    {
        when(logBuffer.capacity()).thenReturn(LOG_BUFFER_CAPACITY);

        logReader = new LogReader(logBuffer, stateBuffer);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCapacityNotMultipleOfAlignment()
    {
        final int logBufferCapacity = LogBufferDescriptor.LOG_MIN_SIZE + FRAME_ALIGNMENT + 1;
        when(logBuffer.capacity()).thenReturn(logBufferCapacity);

        logReader = new LogReader(logBuffer, stateBuffer);
    }

    @Test
    public void shouldReadFirstMessage()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);

        when(logBuffer.getIntVolatile(lengthOffset(0))).thenReturn(frameLength);
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(alignedFrameLength);
        when(logBuffer.getShort(typeOffset(0), LITTLE_ENDIAN)).thenReturn((short)HDR_TYPE_DATA);

        assertThat(logReader.read(Integer.MAX_VALUE, handler), is(1));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(0));
        verify(handler).onFrame(logBuffer, 0, frameLength);
    }

    @Test
    public void shouldNotReadWhenLimitIsZero()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(alignedFrameLength);

        assertThat(logReader.read(0, handler), is(0));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        verifyZeroInteractions(handler);
    }

    @Test
    public void shouldNotReadPastTail()
    {
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(0);

        assertThat(logReader.read(Integer.MAX_VALUE, handler), is(0));

        verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        verify(handler, never()).onFrame(any(), anyInt(), anyInt());
    }


    @Test
    public void shouldReadOneLimitedMessage()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);

        when(logBuffer.getIntVolatile(anyInt())).thenReturn(frameLength);
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(alignedFrameLength * 2);
        when(logBuffer.getShort(anyInt(), any())).thenReturn((short)HDR_TYPE_DATA);

        assertThat(logReader.read(1, handler), is(1));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer, handler);
        inOrder.verify(stateBuffer, times(1)).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(0));
        inOrder.verify(handler).onFrame(logBuffer, 0, frameLength);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldReadMultipleMessages()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);

        when(logBuffer.getIntVolatile(anyInt())).thenReturn(frameLength);
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(alignedFrameLength * 2);
        when(logBuffer.getShort(anyInt(), any())).thenReturn((short)HDR_TYPE_DATA);

        assertThat(logReader.read(Integer.MAX_VALUE, handler), is(2));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer, handler);
        inOrder.verify(stateBuffer, times(1)).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(0));
        inOrder.verify(handler).onFrame(logBuffer, 0, frameLength);

        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(alignedFrameLength));
        inOrder.verify(handler).onFrame(logBuffer, alignedFrameLength, frameLength);
    }

    @Test
    public void shouldReadLastMessage()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int startOfMessage = LOG_BUFFER_CAPACITY - alignedFrameLength;

        when(logBuffer.getIntVolatile(lengthOffset(startOfMessage))).thenReturn(frameLength);
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(LOG_BUFFER_CAPACITY);
        when(logBuffer.getShort(typeOffset(startOfMessage), LITTLE_ENDIAN)).thenReturn((short)HDR_TYPE_DATA);

        logReader.seek(startOfMessage);
        assertThat(logReader.read(Integer.MAX_VALUE, handler), is(1));
        assertTrue(logReader.isComplete());

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(stateBuffer, atLeastOnce()).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(startOfMessage));
        verify(handler).onFrame(logBuffer, startOfMessage, frameLength);
    }

    @Test
    public void shouldNotReadLastMessageWhenPadding()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int startOfMessage = LOG_BUFFER_CAPACITY - alignedFrameLength;

        when(logBuffer.getIntVolatile(lengthOffset(startOfMessage))).thenReturn(frameLength);
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(LOG_BUFFER_CAPACITY);
        when(logBuffer.getShort(typeOffset(startOfMessage), LITTLE_ENDIAN)).thenReturn((short)PADDING_FRAME_TYPE);

        logReader.seek(startOfMessage);
        assertThat(logReader.read(Integer.MAX_VALUE, handler), is(0));
        assertTrue(logReader.isComplete());

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(stateBuffer, atLeastOnce()).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(startOfMessage));
        verify(handler, never()).onFrame(any(), anyInt(), anyInt());
    }
}

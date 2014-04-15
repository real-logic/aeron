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
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import static java.lang.Integer.valueOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.LENGTH_OFFSET;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.TAIL_COUNTER_OFFSET;

public class ReaderTest
{
    private static final int LOG_BUFFER_CAPACITY = 1024 * 16;
    private static final int STATE_BUFFER_CAPACITY = STATE_BUFFER_LENGTH;

    private final AtomicBuffer logBuffer = mock(AtomicBuffer.class);
    private final AtomicBuffer stateBuffer = spy(new AtomicBuffer(new byte[STATE_BUFFER_CAPACITY]));
    private final StateViewer stateViewer = new StateViewer(stateBuffer);
    private final Reader.FrameHandler handler = Mockito.mock(Reader.FrameHandler.class);

    private Reader reader;

    @Before
    public void setUp()
    {
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(LOG_BUFFER_CAPACITY));

        reader = new Reader(logBuffer, stateBuffer);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCapacityNotMultipleOfAlignment()
    {
        final int logBufferCapacity = LogBufferDescriptor.LOG_MIN_SIZE + FRAME_ALIGNMENT + 1;
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(logBufferCapacity));

        reader = new Reader(logBuffer, stateBuffer);
    }

    @Test
    public void readFirstMessage()
    {
        when(logBuffer.getIntVolatile(FrameDescriptor.lengthOffset(0))).thenReturn(FRAME_ALIGNMENT);
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getInt(FrameDescriptor.typeOffset(0))).thenReturn(HeaderFlyweight.HDR_TYPE_NAK);

        assertThat(reader.read(handler), is(1));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(FrameDescriptor.lengthOffset(0));
        verify(handler).onFrame(logBuffer, 0, FRAME_ALIGNMENT);
    }

    @Test
    public void shouldNotReadPastTail()
    {
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(0);

        assertThat(reader.read(handler), is(0));

        verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        verify(handler, never()).onFrame(any(), anyInt(), anyInt());
    }

    @Test
    public void readsMultipleMessages()
    {
        when(logBuffer.getIntVolatile(anyInt())).thenReturn(FRAME_ALIGNMENT);
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(FRAME_ALIGNMENT * 2);
        when(logBuffer.getInt(anyInt())).thenReturn(HeaderFlyweight.HDR_TYPE_NAK);

        assertThat(reader.read(handler), is(2));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer, handler);
        inOrder.verify(stateBuffer, times(1)).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(FrameDescriptor.lengthOffset(0));
        inOrder.verify(handler).onFrame(logBuffer, 0, FRAME_ALIGNMENT);
        inOrder.verify(logBuffer).getIntVolatile(FrameDescriptor.lengthOffset(FRAME_ALIGNMENT));
        inOrder.verify(handler).onFrame(logBuffer, FRAME_ALIGNMENT, FRAME_ALIGNMENT);
    }

    @Test
    public void readsLastMessage()
    {
        final int startOfMessage = LOG_BUFFER_CAPACITY - FRAME_ALIGNMENT;
        when(logBuffer.getIntVolatile(FrameDescriptor.lengthOffset(startOfMessage))).thenReturn(FRAME_ALIGNMENT);
        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET)).thenReturn(LOG_BUFFER_CAPACITY);
        when(logBuffer.getInt(FrameDescriptor.typeOffset(startOfMessage))).thenReturn(HeaderFlyweight.HDR_TYPE_NAK);

        reader.seek(startOfMessage);
        assertThat(reader.read(handler), is(1));

        final InOrder inOrder = inOrder(logBuffer, stateBuffer);
        inOrder.verify(stateBuffer, atLeastOnce()).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(FrameDescriptor.lengthOffset(startOfMessage));
        verify(handler).onFrame(logBuffer, startOfMessage, FRAME_ALIGNMENT);
    }

}

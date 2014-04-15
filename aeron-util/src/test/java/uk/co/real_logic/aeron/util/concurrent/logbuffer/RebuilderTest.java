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
import org.mockito.Mockito;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static java.lang.Integer.parseInt;
import static java.lang.Integer.valueOf;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.lengthOffset;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.HIGH_WATER_MARK_OFFSET;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.TAIL_COUNTER_OFFSET;

public class RebuilderTest
{

    private static final int LOG_BUFFER_CAPACITY = 1024 * 16;
    private static final int STATE_BUFFER_CAPACITY = 1024;

    private final AtomicBuffer logBuffer = mock(AtomicBuffer.class);
    private final AtomicBuffer stateBuffer = mock(AtomicBuffer.class);

    private Rebuilder rebuilder;

    @Before
    public void setUp()
    {
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(LOG_BUFFER_CAPACITY));
        when(valueOf(stateBuffer.capacity())).thenReturn(valueOf(STATE_BUFFER_CAPACITY));

        rebuilder = new Rebuilder(logBuffer, stateBuffer);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCapacityNotMultipleOfAlignment()
    {
        final int logBufferCapacity = LogBufferDescriptor.LOG_MIN_SIZE + FRAME_ALIGNMENT + 1;
        when(valueOf(logBuffer.capacity())).thenReturn(valueOf(logBufferCapacity));

        rebuilder = new Rebuilder(logBuffer, stateBuffer);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientStateBufferCapacity()
    {
        when(valueOf(stateBuffer.capacity())).thenReturn(valueOf(LogBufferDescriptor.STATE_SIZE - 1));

        rebuilder = new Rebuilder(logBuffer, stateBuffer);
    }

    @Test
    public void shouldInsertIntoEmptyBuffer()
    {
        final AtomicBuffer packet = new AtomicBuffer(ByteBuffer.allocate(256));
        int srcOffset = 0;
        int length = 256;

        when(logBuffer.getInt(lengthOffset(0))).thenReturn(length);

        rebuilder.insert(packet, srcOffset, length);

        final InOrder order = inOrder(logBuffer, stateBuffer);
        order.verify(logBuffer).putBytes(0, packet, srcOffset, length);
        order.verify(stateBuffer).putIntOrdered(TAIL_COUNTER_OFFSET, length);
        order.verify(stateBuffer).putIntOrdered(HIGH_WATER_MARK_OFFSET, length);
    }

    @Test
    public void shouldInsertLastFrameIntoBuffer()
    {
        final AtomicBuffer packet = new AtomicBuffer(ByteBuffer.allocate(256));
        int srcOffset = 0;
        int length = FRAME_ALIGNMENT;

        when(stateBuffer.getInt(TAIL_COUNTER_OFFSET)).thenReturn(LOG_BUFFER_CAPACITY - FRAME_ALIGNMENT);
        when(stateBuffer.getInt(HIGH_WATER_MARK_OFFSET)).thenReturn(LOG_BUFFER_CAPACITY - FRAME_ALIGNMENT);

        //rebuilder.insert();

    }
}

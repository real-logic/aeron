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
package uk.co.real_logic.aeron.util.concurrent.ringbuffer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.EventHandler;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.BitUtil.align;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer.*;

public class ManyToOneRingBufferTest
{
    public static final int EVENT_TYPE_ID = 7;
    public static final int CAPACITY = 1024;
    public static final int TOTAL_BUFFER_SIZE = CAPACITY + RingBufferDescriptor.TRAILER_SIZE;
    public static final int TAIL_COUNTER_INDEX = CAPACITY + RingBufferDescriptor.TAIL_COUNTER_OFFSET;
    public static final int HEAD_COUNTER_INDEX = CAPACITY + RingBufferDescriptor.HEAD_COUNTER_OFFSET;

    private final AtomicBuffer atomicBuffer = mock(AtomicBuffer.class);
    private RingBuffer ringBuffer;

    @Before
    public void setUp()
    {
        when(Integer.valueOf(atomicBuffer.capacity())).thenReturn(Integer.valueOf(TOTAL_BUFFER_SIZE));

        ringBuffer = new ManyToOneRingBuffer(atomicBuffer);
    }

    @Test
    public void shouldCalculateCapacityForBuffer()
    {
        assertThat(Integer.valueOf(ringBuffer.capacity()), is(Integer.valueOf(CAPACITY)));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionForCapacityThatIsNotPowerOfTwo()
    {
        final int capacity = 777;
        final int totalBufferSize = capacity + RingBufferDescriptor.TRAILER_SIZE;

        when(Integer.valueOf(atomicBuffer.capacity()))
            .thenReturn(Integer.valueOf(totalBufferSize));

        new ManyToOneRingBuffer(atomicBuffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenMaxEventSizeExceeded()
    {
        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);

        ringBuffer.write(EVENT_TYPE_ID, srcBuffer, 0, ringBuffer.maxEventSize() + 1);
    }

    @Test
    public void shouldWriteToEmptyBuffer()
    {
        final int length = 8;
        final int recordLength = align(length + RECORD_HEADER_SIZE, ALIGNMENT);
        final long tail = 0L;
        final long head = 0L;

        when(Long.valueOf(atomicBuffer.getLongVolatile(HEAD_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(head));
        when(Long.valueOf(atomicBuffer.getLongVolatile(TAIL_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(tail));
        when(Boolean.valueOf(atomicBuffer.compareAndSetLong(TAIL_COUNTER_INDEX, tail, tail + recordLength)))
            .thenReturn(Boolean.TRUE);

        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);

        final int srcIndex = 0;
        assertTrue(ringBuffer.write(EVENT_TYPE_ID, srcBuffer, srcIndex, length));

        final InOrder inOrder = inOrder(atomicBuffer);
        inOrder.verify(atomicBuffer).putInt(lengthOffset((int)tail), recordLength);
        inOrder.verify(atomicBuffer).putBytes(encodedEventOffset((int)tail), srcBuffer, srcIndex, length);
        inOrder.verify(atomicBuffer).putIntOrdered(eventTypeOffset((int)tail), EVENT_TYPE_ID);
    }

    @Test
    public void shouldRejectWriteWhenInsufficientSpace()
    {
        final int length = 200;
        final long head = 0L;
        final long tail = head + (CAPACITY - align(length - ALIGNMENT, ALIGNMENT));

        when(Long.valueOf(atomicBuffer.getLongVolatile(HEAD_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(head));
        when(Long.valueOf(atomicBuffer.getLongVolatile(TAIL_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(tail));

        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);

        final int srcIndex = 0;
        assertFalse(ringBuffer.write(EVENT_TYPE_ID, srcBuffer, srcIndex, length));

        verify(atomicBuffer, never()).putInt(anyInt(), anyInt());
        verify(atomicBuffer, never()).compareAndSetLong(anyInt(), anyLong(), anyLong());
        verify(atomicBuffer, never()).putIntOrdered(anyInt(), anyInt());
    }

    @Test
    public void shouldRejectWriteWhenBufferFull()
    {
        final int length = 8;
        final long head = 0L;
        final long tail = head + CAPACITY;

        when(Long.valueOf(atomicBuffer.getLongVolatile(HEAD_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(head));
        when(Long.valueOf(atomicBuffer.getLongVolatile(TAIL_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(tail));

        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);

        final int srcIndex = 0;
        assertFalse(ringBuffer.write(EVENT_TYPE_ID, srcBuffer, srcIndex, length));

        verify(atomicBuffer, never()).putInt(anyInt(), anyInt());
        verify(atomicBuffer, never()).compareAndSetLong(anyInt(), anyLong(), anyLong());
        verify(atomicBuffer, never()).putIntOrdered(anyInt(), anyInt());
    }

    @Test
    public void shouldInsertPaddingRecordPlusEventOnBufferWrap()
    {
        final int length = 200;
        final int recordLength = align(length + RECORD_HEADER_SIZE, ALIGNMENT);
        final long tail = CAPACITY - ALIGNMENT;
        final long head = tail - (ALIGNMENT * 4);

        when(Long.valueOf(atomicBuffer.getLongVolatile(HEAD_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(head));
        when(Long.valueOf(atomicBuffer.getLongVolatile(TAIL_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(tail));
        when(Boolean.valueOf(atomicBuffer.compareAndSetLong(TAIL_COUNTER_INDEX, tail, tail + recordLength + ALIGNMENT)))
            .thenReturn(Boolean.TRUE);

        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);

        final int srcIndex = 0;
        assertTrue(ringBuffer.write(EVENT_TYPE_ID, srcBuffer, srcIndex, length));

        final InOrder inOrder = inOrder(atomicBuffer);
        inOrder.verify(atomicBuffer).putInt(lengthOffset((int)tail), ALIGNMENT);
        inOrder.verify(atomicBuffer).putIntOrdered(eventTypeOffset((int)tail), PADDING_EVENT_TYPE_ID);

        inOrder.verify(atomicBuffer).putInt(lengthOffset(0), recordLength);
        inOrder.verify(atomicBuffer).putBytes(encodedEventOffset(0), srcBuffer, srcIndex, length);
        inOrder.verify(atomicBuffer).putIntOrdered(eventTypeOffset(0), EVENT_TYPE_ID);
    }

    @Test
    public void shouldReadNothingFromEmptyBuffer()
    {
        final long tail = 0L;
        final long head = 0L;

        when(Long.valueOf(atomicBuffer.getLongVolatile(HEAD_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(head));
        when(Long.valueOf(atomicBuffer.getLongVolatile(TAIL_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(tail));

        final EventHandler handler = (eventTypeId, buffer, index, length) -> fail("should not be called");
        final int eventsRead = ringBuffer.read(handler);

        assertThat(Integer.valueOf(eventsRead), is(Integer.valueOf(0)));
    }

    @Test
    public void shouldReadSingleEventWithAllReadInCorrectMemoryOrder()
    {
        final long tail = ALIGNMENT;
        final long head = 0L;
        final int headIndex = (int)head;

        when(Long.valueOf(atomicBuffer.getLongVolatile(HEAD_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(head));
        when(Long.valueOf(atomicBuffer.getLongVolatile(TAIL_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(tail));
        when(Integer.valueOf(atomicBuffer.getIntVolatile(eventTypeOffset(headIndex))))
            .thenReturn(Integer.valueOf(0))
            .thenReturn(Integer.valueOf(EVENT_TYPE_ID));
        when(Integer.valueOf(atomicBuffer.getInt(lengthOffset(headIndex))))
            .thenReturn(Integer.valueOf(ALIGNMENT));
        when(Integer.valueOf(atomicBuffer.getInt(eventLengthOffset(headIndex))))
            .thenReturn(Integer.valueOf(ALIGNMENT / 2));

        final int[] times = new int[1];
        final EventHandler handler = (eventTypeId, buffer, index, length) -> times[0]++;
        final int eventsRead = ringBuffer.read(handler);

        assertThat(Integer.valueOf(eventsRead), is(Integer.valueOf(1)));
        assertThat(Integer.valueOf(times[0]), is(Integer.valueOf(1)));

        final InOrder inOrder = inOrder(atomicBuffer);
        inOrder.verify(atomicBuffer, times(2)).getIntVolatile(eventTypeOffset(headIndex));
        inOrder.verify(atomicBuffer).getInt(lengthOffset(headIndex));
        inOrder.verify(atomicBuffer).getInt(eventLengthOffset(headIndex));

        inOrder.verify(atomicBuffer, times(1)).setMemory(headIndex, ALIGNMENT, (byte)0);
        inOrder.verify(atomicBuffer, times(1)).putLongOrdered(HEAD_COUNTER_INDEX, tail);
    }

    @Test
    public void shouldReadTwoEvents()
    {
        final long tail = ALIGNMENT * 2;
        final long head = 0L;
        final int headIndex = (int)head;

        when(Long.valueOf(atomicBuffer.getLongVolatile(HEAD_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(head));
        when(Long.valueOf(atomicBuffer.getLongVolatile(TAIL_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(tail));
        when(Integer.valueOf(atomicBuffer.getIntVolatile(eventTypeOffset(headIndex))))
            .thenReturn(Integer.valueOf(EVENT_TYPE_ID));
        when(Integer.valueOf(atomicBuffer.getIntVolatile(eventTypeOffset(headIndex + ALIGNMENT))))
            .thenReturn(Integer.valueOf(EVENT_TYPE_ID));
        when(Integer.valueOf(atomicBuffer.getInt(lengthOffset(headIndex))))
            .thenReturn(Integer.valueOf(ALIGNMENT));
        when(Integer.valueOf(atomicBuffer.getInt(lengthOffset(headIndex + ALIGNMENT))))
            .thenReturn(Integer.valueOf(ALIGNMENT));

        final int[] times = new int[1];
        final EventHandler handler = (eventTypeId, buffer, index, length) -> times[0]++;
        final int eventsRead = ringBuffer.read(handler);

        assertThat(Integer.valueOf(eventsRead), is(Integer.valueOf(2)));
        assertThat(Integer.valueOf(times[0]), is(Integer.valueOf(2)));

        final InOrder inOrder = inOrder(atomicBuffer);
        inOrder.verify(atomicBuffer, times(1)).setMemory(headIndex, ALIGNMENT * 2, (byte)0);
        inOrder.verify(atomicBuffer, times(1)).putLongOrdered(HEAD_COUNTER_INDEX, tail);
    }

    @Test
    public void shouldCopeWithExceptionFromHandler()
    {
        final long tail = ALIGNMENT * 2;
        final long head = 0L;
        final int headIndex = (int)head;

        when(Long.valueOf(atomicBuffer.getLongVolatile(HEAD_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(head));
        when(Long.valueOf(atomicBuffer.getLongVolatile(TAIL_COUNTER_INDEX)))
            .thenReturn(Long.valueOf(tail));
        when(Integer.valueOf(atomicBuffer.getIntVolatile(eventTypeOffset(headIndex))))
            .thenReturn(Integer.valueOf(EVENT_TYPE_ID));
        when(Integer.valueOf(atomicBuffer.getIntVolatile(eventTypeOffset(headIndex + ALIGNMENT))))
            .thenReturn(Integer.valueOf(EVENT_TYPE_ID));
        when(Integer.valueOf(atomicBuffer.getInt(lengthOffset(headIndex))))
            .thenReturn(Integer.valueOf(ALIGNMENT));
        when(Integer.valueOf(atomicBuffer.getInt(lengthOffset(headIndex + ALIGNMENT))))
            .thenReturn(Integer.valueOf(ALIGNMENT));

        final int[] times = new int[1];
        final EventHandler handler =
            (eventTypeId, buffer, index, length) ->
            {
                times[0]++;
                if (times[0] == 2)
                {
                    throw new RuntimeException();
                }
            };

        try
        {
            ringBuffer.read(handler);
        }
        catch (final RuntimeException ignore)
        {
            assertThat(Integer.valueOf(times[0]), is(Integer.valueOf(2)));

            final InOrder inOrder = inOrder(atomicBuffer);
            inOrder.verify(atomicBuffer, times(1)).setMemory(headIndex, ALIGNMENT * 2, (byte)0);
            inOrder.verify(atomicBuffer, times(1)).putLongOrdered(HEAD_COUNTER_INDEX, tail);

            return;
        }

        fail("Should have thrown exception");
    }
}

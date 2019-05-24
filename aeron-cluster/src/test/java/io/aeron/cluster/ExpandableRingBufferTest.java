/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.cluster;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ExpandableRingBufferTest
{
    private static final int MSG_LENGTH_ONE = 250;
    private static final int MSG_LENGTH_TWO = 504;
    private static final UnsafeBuffer TEST_MSG = new UnsafeBuffer(new byte[MSG_LENGTH_TWO]);

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionForNegativeInitialCapacity()
    {
        new ExpandableRingBuffer(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionForOverMaxInitialCapacity()
    {
        new ExpandableRingBuffer(ExpandableRingBuffer.MAX_CAPACITY + 1);
    }

    @Test
    public void shouldAppendMessage()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();
        assertTrue(ringBuffer.isEmpty());

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);
        assertThat(ringBuffer.size(), greaterThan(MSG_LENGTH_ONE));
        assertFalse(ringBuffer.isEmpty());
    }

    @Test
    public void shouldResetCapacity()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();

        ringBuffer.reset(250);
        assertThat(ringBuffer.capacity(), is(BitUtil.findNextPositivePowerOfTwo(250)));
        assertTrue(ringBuffer.isEmpty());
    }

    @Test
    public void shouldResetCapacityToSame()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);

        final int existingCapacity = ringBuffer.capacity();
        ringBuffer.reset(existingCapacity);
        assertThat(ringBuffer.capacity(), is(existingCapacity));
        assertTrue(ringBuffer.isEmpty());
    }

    @Test
    public void shouldResetCapacityUpwards()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);

        final int existingCapacity = ringBuffer.capacity();
        ringBuffer.reset(existingCapacity + 1);
        assertThat(ringBuffer.capacity(), greaterThan(existingCapacity));
        assertTrue(ringBuffer.isEmpty());
    }

    @Test
    public void shouldResetCapacityDownwards()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);

        final int existingCapacity = ringBuffer.capacity();
        ringBuffer.reset((existingCapacity / 2) - 1);
        assertThat(ringBuffer.capacity(), lessThan(existingCapacity));
        assertTrue(ringBuffer.isEmpty());
    }

    @Test
    public void shouldAppendThenConsumeMessage()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();
        final ExpandableRingBuffer.MessageConsumer mockConsumer = mock(ExpandableRingBuffer.MessageConsumer.class);
        when(mockConsumer.onMessage(any(), anyInt(), anyInt())).thenReturn(Boolean.TRUE);

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);

        final int readCount = ringBuffer.consume(mockConsumer, Integer.MAX_VALUE);
        assertThat(readCount, is(1));
        assertTrue(ringBuffer.isEmpty());

        verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_ONE));
    }

    @Test
    public void shouldAppendThenConsumeMessagesInOrder()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();
        final ExpandableRingBuffer.MessageConsumer mockConsumer = mock(ExpandableRingBuffer.MessageConsumer.class);
        when(mockConsumer.onMessage(any(), anyInt(), anyInt())).thenReturn(Boolean.TRUE);

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);
        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_TWO);

        final int readCount = ringBuffer.consume(mockConsumer, Integer.MAX_VALUE);
        assertThat(readCount, is(2));
        assertTrue(ringBuffer.isEmpty());

        final InOrder inOrder = Mockito.inOrder(mockConsumer);
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_ONE));
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_TWO));
    }

    @Test
    public void shouldAppendThenIterateMessagesInOrder()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();
        final ExpandableRingBuffer.MessageConsumer mockConsumer = mock(ExpandableRingBuffer.MessageConsumer.class);
        when(mockConsumer.onMessage(any(), anyInt(), anyInt())).thenReturn(Boolean.TRUE);

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);
        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_TWO);

        final int existingSize = ringBuffer.size();
        ringBuffer.forEach(mockConsumer);
        assertFalse(ringBuffer.isEmpty());
        assertThat(ringBuffer.size(), is(existingSize));

        final InOrder inOrder = Mockito.inOrder(mockConsumer);
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_ONE));
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_TWO));
    }

    @Test
    public void shouldAppendMessagesWithPaddingWithoutExpanding()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();
        final ExpandableRingBuffer.MessageConsumer mockConsumer = mock(ExpandableRingBuffer.MessageConsumer.class);
        when(mockConsumer.onMessage(any(), anyInt(), anyInt())).thenReturn(Boolean.TRUE);

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);
        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_TWO);

        int readCount = ringBuffer.consume(mockConsumer, 1);
        assertThat(readCount, is(1));

        final int existingCapacity = ringBuffer.capacity();
        final int existingSize = ringBuffer.size();
        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);

        assertThat(ringBuffer.capacity(), is(existingCapacity));
        assertThat(ringBuffer.size(), greaterThan(existingSize));

        readCount = ringBuffer.consume(mockConsumer, Integer.MAX_VALUE);
        assertThat(readCount, is(2));

        final InOrder inOrder = Mockito.inOrder(mockConsumer);
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_ONE));
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_TWO));
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_ONE));
        inOrder.verifyNoMoreInteractions();
    }
}
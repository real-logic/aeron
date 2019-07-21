/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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

import static io.aeron.cluster.ExpandableRingBuffer.HEADER_ALIGNMENT;
import static io.aeron.cluster.ExpandableRingBuffer.HEADER_LENGTH;
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
    private static final int MSG_LENGTH_THREE = 700;
    private static final UnsafeBuffer TEST_MSG = new UnsafeBuffer(new byte[MSG_LENGTH_THREE]);

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionForNegativeInitialCapacity()
    {
        new ExpandableRingBuffer(-1, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionForOverMaxInitialCapacity()
    {
        new ExpandableRingBuffer(ExpandableRingBuffer.MAX_CAPACITY + 1, true);
    }

    @Test
    public void shouldDefaultDirectMessage()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();
        assertTrue(ringBuffer.isDirect());
        assertTrue(ringBuffer.isEmpty());
        assertEquals(0L, ringBuffer.head());
        assertEquals(0L, ringBuffer.tail());
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
        when(mockConsumer.onMessage(any(), anyInt(), anyInt(), anyInt())).thenReturn(Boolean.TRUE);

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);

        final int bytes = ringBuffer.consume(mockConsumer, Integer.MAX_VALUE);
        assertThat(bytes, is(BitUtil.align(MSG_LENGTH_ONE + HEADER_LENGTH, HEADER_ALIGNMENT)));
        assertTrue(ringBuffer.isEmpty());

        verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_ONE), anyInt());
    }

    @Test
    public void shouldAppendThenConsumeMessagesInOrder()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();
        final ExpandableRingBuffer.MessageConsumer mockConsumer = mock(ExpandableRingBuffer.MessageConsumer.class);
        when(mockConsumer.onMessage(any(), anyInt(), anyInt(), anyInt())).thenReturn(Boolean.TRUE);

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);
        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_TWO);

        final int bytes = ringBuffer.consume(mockConsumer, Integer.MAX_VALUE);
        final int expectedBytes =
            BitUtil.align(MSG_LENGTH_ONE + HEADER_LENGTH, HEADER_ALIGNMENT) +
            BitUtil.align(MSG_LENGTH_TWO + HEADER_LENGTH, HEADER_ALIGNMENT);
        assertEquals(expectedBytes, bytes);
        assertTrue(ringBuffer.isEmpty());

        final InOrder inOrder = Mockito.inOrder(mockConsumer);
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_ONE), anyInt());
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_TWO), anyInt());
    }

    @Test
    public void shouldAppendThenIterateMessagesInOrder()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();
        final ExpandableRingBuffer.MessageConsumer mockConsumer = mock(ExpandableRingBuffer.MessageConsumer.class);
        when(mockConsumer.onMessage(any(), anyInt(), anyInt(), anyInt())).thenReturn(Boolean.TRUE);

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);
        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_TWO);

        final int existingSize = ringBuffer.size();
        ringBuffer.forEach(mockConsumer, Integer.MAX_VALUE);
        assertFalse(ringBuffer.isEmpty());
        assertThat(ringBuffer.size(), is(existingSize));

        final InOrder inOrder = Mockito.inOrder(mockConsumer);
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_ONE), anyInt());
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_TWO), anyInt());
    }

    @Test
    public void shouldAppendMessagesWithPaddingWithoutExpanding()
    {
        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();
        final ExpandableRingBuffer.MessageConsumer mockConsumer = mock(ExpandableRingBuffer.MessageConsumer.class);
        when(mockConsumer.onMessage(any(), anyInt(), anyInt(), anyInt())).thenReturn(Boolean.TRUE);

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);
        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_TWO);

        int bytes = ringBuffer.consume(mockConsumer, 1);
        assertThat(bytes, is(BitUtil.align(MSG_LENGTH_ONE + HEADER_LENGTH, HEADER_ALIGNMENT)));

        final int existingCapacity = ringBuffer.capacity();
        final int existingSize = ringBuffer.size();

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);

        assertThat(ringBuffer.capacity(), is(existingCapacity));
        assertThat(ringBuffer.size(), greaterThan(existingSize));

        bytes = ringBuffer.consume(mockConsumer, Integer.MAX_VALUE);
        final int expectedBytes =
            BitUtil.align(MSG_LENGTH_ONE + HEADER_LENGTH, HEADER_ALIGNMENT) +
            BitUtil.align(MSG_LENGTH_TWO + HEADER_LENGTH, HEADER_ALIGNMENT);
        assertThat(bytes, greaterThan(expectedBytes));

        final InOrder inOrder = Mockito.inOrder(mockConsumer);
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_ONE), anyInt());
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_TWO), anyInt());
        inOrder.verify(mockConsumer).onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_ONE), anyInt());
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldIterateFromOffsetHeadWithExpansionDueToAppend()
    {
        final int alignedLengthOne = BitUtil.align(MSG_LENGTH_ONE + HEADER_LENGTH, HEADER_ALIGNMENT);
        final int alignedLengthTwo = BitUtil.align(MSG_LENGTH_TWO + HEADER_LENGTH, HEADER_ALIGNMENT);
        final int alignedLengthThree = BitUtil.align(MSG_LENGTH_THREE + HEADER_LENGTH, HEADER_ALIGNMENT);

        final ExpandableRingBuffer ringBuffer = new ExpandableRingBuffer();
        final ExpandableRingBuffer.MessageConsumer mockConsumer = mock(ExpandableRingBuffer.MessageConsumer.class);
        when(mockConsumer.onMessage(any(), anyInt(), anyInt(), anyInt())).thenReturn(Boolean.TRUE);

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_ONE);
        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_TWO);

        final int bytes = ringBuffer.consume(mockConsumer, 1);
        assertThat(bytes, is(alignedLengthOne));

        final int bytesTwo = ringBuffer.forEach(0, mockConsumer, 1);
        assertEquals(alignedLengthTwo, bytesTwo);

        ringBuffer.append(TEST_MSG, 0, MSG_LENGTH_THREE);

        final int bytesThree = ringBuffer.forEach(bytesTwo, mockConsumer, Integer.MAX_VALUE);
        assertThat(bytesThree, is(alignedLengthThree));

        final int expectedOffsetThree = bytesTwo + alignedLengthThree;
        final InOrder inOrder = Mockito.inOrder(mockConsumer);
        inOrder.verify(mockConsumer)
            .onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_ONE), eq(alignedLengthOne));
        inOrder.verify(mockConsumer)
            .onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_TWO), eq(alignedLengthTwo));
        inOrder.verify(mockConsumer)
            .onMessage(any(MutableDirectBuffer.class), anyInt(), eq(MSG_LENGTH_THREE), eq(expectedOffsetThree));
        inOrder.verifyNoMoreInteractions();
    }
}

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
package uk.co.real_logic.agrona.concurrent.broadcast;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.agrona.BitUtil.align;
import static uk.co.real_logic.agrona.concurrent.broadcast.RecordDescriptor.*;

public class BroadcastTransmitterTest
{
    private static final int MSG_TYPE_ID = 7;
    private static final int CAPACITY = 1024;
    private static final int TOTAL_BUFFER_SIZE = CAPACITY + BroadcastBufferDescriptor.TRAILER_LENGTH;
    private static final int TAIL_COUNTER_INDEX = CAPACITY + BroadcastBufferDescriptor.TAIL_COUNTER_OFFSET;
    private static final int LATEST_COUNTER_INDEX = CAPACITY + BroadcastBufferDescriptor.LATEST_COUNTER_OFFSET;

    private final UnsafeBuffer buffer = mock(UnsafeBuffer.class);
    private BroadcastTransmitter broadcastTransmitter;

    @Before
    public void setUp()
    {
        when(buffer.capacity()).thenReturn(TOTAL_BUFFER_SIZE);

        broadcastTransmitter = new BroadcastTransmitter(buffer);
    }

    @Test
    public void shouldCalculateCapacityForBuffer()
    {
        assertThat(broadcastTransmitter.capacity(), is(CAPACITY));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionForCapacityThatIsNotPowerOfTwo()
    {
        final int capacity = 777;
        final int totalBufferSize = capacity + BroadcastBufferDescriptor.TRAILER_LENGTH;

        when(buffer.capacity()).thenReturn(totalBufferSize);

        new BroadcastTransmitter(buffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenMaxMessageSizeExceeded()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);

        broadcastTransmitter.transmit(MSG_TYPE_ID, srcBuffer, 0, broadcastTransmitter.maxMsgLength() + 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenMessageTypeIdInvalid()
    {
        final int invalidMsgId = -1;
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);

        broadcastTransmitter.transmit(invalidMsgId, srcBuffer, 0, 32);
    }

    @Test
    public void shouldTransmitIntoEmptyBuffer()
    {
        final long tail = 0L;
        final int recordOffset = (int)tail;
        final int length = 8;
        final int recordLength = align(length + HEADER_LENGTH, RECORD_ALIGNMENT);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
        final int srcIndex = 0;

        broadcastTransmitter.transmit(MSG_TYPE_ID, srcBuffer, srcIndex, length);

        final InOrder inOrder = inOrder(buffer);
        inOrder.verify(buffer).getLong(TAIL_COUNTER_INDEX);
        inOrder.verify(buffer).putLongOrdered(tailSequenceOffset(recordOffset), tail);
        inOrder.verify(buffer).putInt(recLengthOffset(recordOffset), recordLength);
        inOrder.verify(buffer).putInt(msgLengthOffset(recordOffset), length);
        inOrder.verify(buffer).putInt(msgTypeOffset(recordOffset), MSG_TYPE_ID);
        inOrder.verify(buffer).putBytes(msgOffset(recordOffset), srcBuffer, srcIndex, length);

        inOrder.verify(buffer).putLong(LATEST_COUNTER_INDEX, tail);
        inOrder.verify(buffer).putLongOrdered(TAIL_COUNTER_INDEX, tail + recordLength);
    }

    @Test
    public void shouldTransmitIntoUsedBuffer()
    {
        final long tail = RECORD_ALIGNMENT * 3;
        final int recordOffset = (int)tail;
        final int length = 8;
        final int recordLength = align(length + HEADER_LENGTH, RECORD_ALIGNMENT);

        when(buffer.getLong(TAIL_COUNTER_INDEX)).thenReturn(tail);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
        final int srcIndex = 0;

        broadcastTransmitter.transmit(MSG_TYPE_ID, srcBuffer, srcIndex, length);

        final InOrder inOrder = inOrder(buffer);
        inOrder.verify(buffer).getLong(TAIL_COUNTER_INDEX);

        inOrder.verify(buffer).putLongOrdered(tailSequenceOffset(recordOffset), tail);
        inOrder.verify(buffer).putInt(recLengthOffset(recordOffset), recordLength);
        inOrder.verify(buffer).putInt(msgLengthOffset(recordOffset), length);
        inOrder.verify(buffer).putInt(msgTypeOffset(recordOffset), MSG_TYPE_ID);
        inOrder.verify(buffer).putBytes(msgOffset(recordOffset), srcBuffer, srcIndex, length);

        inOrder.verify(buffer).putLong(LATEST_COUNTER_INDEX, tail);
        inOrder.verify(buffer).putLongOrdered(TAIL_COUNTER_INDEX, tail + recordLength);
    }

    @Test
    public void shouldTransmitIntoEndOfBuffer()
    {
        final long tail = CAPACITY - RECORD_ALIGNMENT;
        final int recordOffset = (int)tail;
        final int length = 8;
        final int recordLength = align(length + HEADER_LENGTH, RECORD_ALIGNMENT);

        when(buffer.getLong(TAIL_COUNTER_INDEX)).thenReturn(tail);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
        final int srcIndex = 0;

        broadcastTransmitter.transmit(MSG_TYPE_ID, srcBuffer, srcIndex, length);

        final InOrder inOrder = inOrder(buffer);
        inOrder.verify(buffer).getLong(TAIL_COUNTER_INDEX);

        inOrder.verify(buffer).putLongOrdered(tailSequenceOffset(recordOffset), tail);
        inOrder.verify(buffer).putInt(recLengthOffset(recordOffset), recordLength);
        inOrder.verify(buffer).putInt(msgLengthOffset(recordOffset), length);
        inOrder.verify(buffer).putInt(msgTypeOffset(recordOffset), MSG_TYPE_ID);
        inOrder.verify(buffer).putBytes(msgOffset(recordOffset), srcBuffer, srcIndex, length);

        inOrder.verify(buffer).putLong(LATEST_COUNTER_INDEX, tail);
        inOrder.verify(buffer).putLongOrdered(TAIL_COUNTER_INDEX, tail + recordLength);
    }

    @Test
    public void shouldApplyPaddingWhenInsufficientSpaceAtEndOfBuffer()
    {
        long tail = CAPACITY - RECORD_ALIGNMENT;
        int recordOffset = (int)tail;
        final int length = RECORD_ALIGNMENT + 8;
        final int recordLength = align(length + HEADER_LENGTH, RECORD_ALIGNMENT);

        when(buffer.getLong(TAIL_COUNTER_INDEX)).thenReturn(tail);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
        final int srcIndex = 0;

        broadcastTransmitter.transmit(MSG_TYPE_ID, srcBuffer, srcIndex, length);

        final InOrder inOrder = inOrder(buffer);
        inOrder.verify(buffer).getLong(TAIL_COUNTER_INDEX);

        inOrder.verify(buffer).putLongOrdered(tailSequenceOffset(recordOffset), tail);
        inOrder.verify(buffer).putInt(recLengthOffset(recordOffset), CAPACITY - recordOffset);
        inOrder.verify(buffer).putInt(msgLengthOffset(recordOffset), 0);
        inOrder.verify(buffer).putInt(msgTypeOffset(recordOffset), PADDING_MSG_TYPE_ID);

        tail += (CAPACITY - recordOffset);
        recordOffset = 0;
        inOrder.verify(buffer).putLongOrdered(tailSequenceOffset(recordOffset), tail);
        inOrder.verify(buffer).putInt(recLengthOffset(recordOffset), recordLength);
        inOrder.verify(buffer).putInt(msgLengthOffset(recordOffset), length);
        inOrder.verify(buffer).putInt(msgTypeOffset(recordOffset), MSG_TYPE_ID);
        inOrder.verify(buffer).putBytes(msgOffset(recordOffset), srcBuffer, srcIndex, length);

        inOrder.verify(buffer).putLong(LATEST_COUNTER_INDEX, tail);
        inOrder.verify(buffer).putLongOrdered(TAIL_COUNTER_INDEX, tail + recordLength);
    }
}

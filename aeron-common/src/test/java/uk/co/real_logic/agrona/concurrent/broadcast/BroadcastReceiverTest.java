package uk.co.real_logic.agrona.concurrent.broadcast;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.agrona.BitUtil.align;
import static uk.co.real_logic.agrona.concurrent.broadcast.RecordDescriptor.*;

public class BroadcastReceiverTest
{
    private static final int MSG_TYPE_ID = 7;
    private static final int CAPACITY = 1024;
    private static final int TOTAL_BUFFER_SIZE = CAPACITY + BroadcastBufferDescriptor.TRAILER_LENGTH;
    private static final int TAIL_COUNTER_INDEX = CAPACITY + BroadcastBufferDescriptor.TAIL_COUNTER_OFFSET;
    private static final int LATEST_COUNTER_INDEX = CAPACITY + BroadcastBufferDescriptor.LATEST_COUNTER_OFFSET;

    private final UnsafeBuffer buffer = mock(UnsafeBuffer.class);
    private BroadcastReceiver broadcastReceiver;

    @Before
    public void setUp()
    {
        when(buffer.capacity()).thenReturn(TOTAL_BUFFER_SIZE);

        broadcastReceiver = new BroadcastReceiver(buffer);
    }

    @Test
    public void shouldCalculateCapacityForBuffer()
    {
        assertThat(broadcastReceiver.capacity(), is(CAPACITY));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionForCapacityThatIsNotPowerOfTwo()
    {
        final int capacity = 777;
        final int totalBufferSize = capacity + BroadcastBufferDescriptor.TRAILER_LENGTH;

        when(buffer.capacity()).thenReturn(totalBufferSize);

        new BroadcastReceiver(buffer);
    }

    @Test
    public void shouldNotBeLappedBeforeReception()
    {
        assertThat(broadcastReceiver.lappedCount(), is(0L));
    }

    @Test
    public void shouldNotReceiveFromEmptyBuffer()
    {
        assertFalse(broadcastReceiver.receiveNext());
    }

    @Test
    public void shouldReceiveFirstMessageFromBuffer()
    {
        final int length = 8;
        final int recordLength = align(length + HEADER_LENGTH, RECORD_ALIGNMENT);
        final long tail = recordLength;
        final long latestRecord = tail - recordLength;
        final int recordOffset = (int)latestRecord;

        when(buffer.getLongVolatile(TAIL_COUNTER_INDEX)).thenReturn(tail);
        when(buffer.getLongVolatile(LATEST_COUNTER_INDEX)).thenReturn(latestRecord);
        when(buffer.getLongVolatile(tailSequenceOffset(recordOffset))).thenReturn(latestRecord);
        when(buffer.getInt(recLengthOffset(recordOffset))).thenReturn(recordLength);
        when(buffer.getInt(msgLengthOffset(recordOffset))).thenReturn(length);
        when(buffer.getInt(msgTypeOffset(recordOffset))).thenReturn(MSG_TYPE_ID);

        assertTrue(broadcastReceiver.receiveNext());
        assertThat(broadcastReceiver.typeId(), is(MSG_TYPE_ID));
        assertThat(broadcastReceiver.buffer(), is(buffer));
        assertThat(broadcastReceiver.offset(), is(msgOffset(recordOffset)));
        assertThat(broadcastReceiver.length(), is(length));

        assertTrue(broadcastReceiver.validate());

        final InOrder inOrder = inOrder(buffer);
        inOrder.verify(buffer).getLongVolatile(TAIL_COUNTER_INDEX);
        inOrder.verify(buffer).getLongVolatile(tailSequenceOffset(recordOffset));
    }

    @Test
    public void shouldReceiveTwoMessagesFromBuffer()
    {
        final int length = 8;
        final int recordLength = align(length + HEADER_LENGTH, RECORD_ALIGNMENT);
        final long tail = recordLength * 2;
        final long latestRecord = tail - recordLength;
        final int recordOffsetOne = 0;
        final int recordOffsetTwo = (int)latestRecord;

        when(buffer.getLongVolatile(TAIL_COUNTER_INDEX)).thenReturn(tail);
        when(buffer.getLongVolatile(LATEST_COUNTER_INDEX)).thenReturn(latestRecord);

        when(buffer.getLongVolatile(tailSequenceOffset(recordOffsetOne))).thenReturn(0L);
        when(buffer.getInt(recLengthOffset(recordOffsetOne))).thenReturn(recordLength);
        when(buffer.getInt(msgLengthOffset(recordOffsetOne))).thenReturn(length);
        when(buffer.getInt(msgTypeOffset(recordOffsetOne))).thenReturn(MSG_TYPE_ID);

        when(buffer.getLongVolatile(tailSequenceOffset(recordOffsetTwo))).thenReturn(latestRecord);
        when(buffer.getInt(recLengthOffset(recordOffsetTwo))).thenReturn(recordLength);
        when(buffer.getInt(msgLengthOffset(recordOffsetTwo))).thenReturn(length);
        when(buffer.getInt(msgTypeOffset(recordOffsetTwo))).thenReturn(MSG_TYPE_ID);

        assertTrue(broadcastReceiver.receiveNext());
        assertThat(broadcastReceiver.typeId(), is(MSG_TYPE_ID));
        assertThat(broadcastReceiver.buffer(), is(buffer));
        assertThat(broadcastReceiver.offset(), is(msgOffset(recordOffsetOne)));
        assertThat(broadcastReceiver.length(), is(length));

        assertTrue(broadcastReceiver.validate());

        assertTrue(broadcastReceiver.receiveNext());
        assertThat(broadcastReceiver.typeId(), is(MSG_TYPE_ID));
        assertThat(broadcastReceiver.buffer(), is(buffer));
        assertThat(broadcastReceiver.offset(), is(msgOffset(recordOffsetTwo)));
        assertThat(broadcastReceiver.length(), is(length));

        assertTrue(broadcastReceiver.validate());
    }

    @Test
    public void shouldLateJoinTransmission()
    {
        final int length = 8;
        final int recordLength = align(length + HEADER_LENGTH, RECORD_ALIGNMENT);
        final long tail = (CAPACITY * 3L) + RECORD_ALIGNMENT + recordLength;
        final long latestRecord = tail - recordLength;
        final int recordOffset = (int)latestRecord & (CAPACITY - 1);

        when(buffer.getLongVolatile(TAIL_COUNTER_INDEX)).thenReturn(tail);
        when(buffer.getLongVolatile(LATEST_COUNTER_INDEX)).thenReturn(latestRecord);

        when(buffer.getLongVolatile(tailSequenceOffset(0))).thenReturn(CAPACITY * 3L);

        when(buffer.getLongVolatile(tailSequenceOffset(recordOffset))).thenReturn(latestRecord);
        when(buffer.getInt(recLengthOffset(recordOffset))).thenReturn(recordLength);
        when(buffer.getInt(msgLengthOffset(recordOffset))).thenReturn(length);
        when(buffer.getInt(msgTypeOffset(recordOffset))).thenReturn(MSG_TYPE_ID);

        assertTrue(broadcastReceiver.receiveNext());
        assertThat(broadcastReceiver.typeId(), is(MSG_TYPE_ID));
        assertThat(broadcastReceiver.buffer(), is(buffer));
        assertThat(broadcastReceiver.offset(), is(msgOffset(recordOffset)));
        assertThat(broadcastReceiver.length(), is(length));

        assertTrue(broadcastReceiver.validate());
        assertThat(broadcastReceiver.lappedCount(), is(greaterThan(0L)));
    }

    @Test
    public void shouldCopeWithPaddingRecordAndWrapOfBufferToNextRecord()
    {
        final int length = 120;
        final int recordLength = align(length + HEADER_LENGTH, RECORD_ALIGNMENT);
        final long catchupTail = (CAPACITY * 2L) - RECORD_ALIGNMENT;
        final long postPaddingTail = catchupTail + RECORD_ALIGNMENT + recordLength;
        final long latestRecord = catchupTail - recordLength;
        final int catchupOffset = (int)latestRecord & (CAPACITY - 1);

        when(buffer.getLongVolatile(TAIL_COUNTER_INDEX)).thenReturn(catchupTail)
                                                        .thenReturn(postPaddingTail);
        when(buffer.getLongVolatile(LATEST_COUNTER_INDEX)).thenReturn(latestRecord);

        when(buffer.getLongVolatile(tailSequenceOffset(0))).thenReturn(CAPACITY * 2L);

        when(buffer.getLongVolatile(tailSequenceOffset(catchupOffset))).thenReturn(latestRecord);
        when(buffer.getInt(recLengthOffset(catchupOffset))).thenReturn(recordLength);
        when(buffer.getInt(msgLengthOffset(catchupOffset))).thenReturn(length);
        when(buffer.getInt(msgTypeOffset(catchupOffset))).thenReturn(MSG_TYPE_ID);

        final int paddingOffset = (int)catchupTail & (CAPACITY - 1);
        final int recordOffset = (int)(postPaddingTail - recordLength) & (CAPACITY - 1);
        when(buffer.getLongVolatile(tailSequenceOffset(paddingOffset))).thenReturn(catchupTail);
        when(buffer.getInt(recLengthOffset(paddingOffset))).thenReturn(RECORD_ALIGNMENT);
        when(buffer.getInt(msgTypeOffset(paddingOffset))).thenReturn(PADDING_MSG_TYPE_ID);

        when(buffer.getLongVolatile(tailSequenceOffset(recordOffset))).thenReturn(postPaddingTail - recordLength);
        when(buffer.getInt(recLengthOffset(recordOffset))).thenReturn(recordLength);
        when(buffer.getInt(msgLengthOffset(recordOffset))).thenReturn(length);
        when(buffer.getInt(msgTypeOffset(recordOffset))).thenReturn(MSG_TYPE_ID);

        assertTrue(broadcastReceiver.receiveNext()); // To catch up to record before padding.

        assertTrue(broadcastReceiver.receiveNext()); // no skip over the padding and read next record.
        assertThat(broadcastReceiver.typeId(), is(MSG_TYPE_ID));
        assertThat(broadcastReceiver.buffer(), is(buffer));
        assertThat(broadcastReceiver.offset(), is(msgOffset(recordOffset)));
        assertThat(broadcastReceiver.length(), is(length));

        assertTrue(broadcastReceiver.validate());
    }

    @Test
    public void shouldDealWithRecordBecomingInvalidDueToOverwrite()
    {
        final int length = 8;
        final int recordLength = align(length + HEADER_LENGTH, RECORD_ALIGNMENT);
        final long tail = recordLength;
        final long latestRecord = tail - recordLength;
        final int recordOffset = (int)latestRecord;

        when(buffer.getLongVolatile(TAIL_COUNTER_INDEX)).thenReturn(tail);
        when(buffer.getLongVolatile(LATEST_COUNTER_INDEX)).thenReturn(latestRecord);
        when(buffer.getLongVolatile(tailSequenceOffset(recordOffset))).thenReturn(latestRecord)
                                                                      .thenReturn(latestRecord + CAPACITY);
        when(buffer.getInt(recLengthOffset(recordOffset))).thenReturn(recordLength);
        when(buffer.getInt(msgLengthOffset(recordOffset))).thenReturn(length);
        when(buffer.getInt(msgTypeOffset(recordOffset))).thenReturn(MSG_TYPE_ID);

        assertTrue(broadcastReceiver.receiveNext());
        assertThat(broadcastReceiver.typeId(), is(MSG_TYPE_ID));
        assertThat(broadcastReceiver.buffer(), is(buffer));
        assertThat(broadcastReceiver.offset(), is(msgOffset(recordOffset)));
        assertThat(broadcastReceiver.length(), is(length));

        assertFalse(broadcastReceiver.validate()); // Need to receiveNext() to catch up with transmission again.

        final InOrder inOrder = inOrder(buffer);
        inOrder.verify(buffer).getLongVolatile(TAIL_COUNTER_INDEX);
        inOrder.verify(buffer).getLongVolatile(tailSequenceOffset(recordOffset));
        inOrder.verify(buffer).getLongVolatile(tailSequenceOffset(recordOffset));
    }
}
package uk.co.real_logic.aeron.util.concurrent.broadcast;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.BitUtil.align;
import static uk.co.real_logic.aeron.util.concurrent.broadcast.RecordDescriptor.*;

public class ReceiverTest
{
    public static final int MSG_TYPE_ID = 7;
    public static final int CAPACITY = 1024;
    public static final int TOTAL_BUFFER_SIZE = CAPACITY + BufferDescriptor.TRAILER_SIZE;
    public static final int TAIL_COUNTER_INDEX = CAPACITY + BufferDescriptor.TAIL_COUNTER_OFFSET;
    public static final int LATEST_COUNTER_INDEX = CAPACITY + BufferDescriptor.LATEST_COUNTER_OFFSET;

    private final AtomicBuffer buffer = mock(AtomicBuffer.class);
    private Receiver receiver;

    @Before
    public void setUp()
    {
        when(buffer.capacity()).thenReturn(TOTAL_BUFFER_SIZE);

        receiver = new Receiver(buffer);
    }

    @Test
    public void shouldCalculateCapacityForBuffer()
    {
        assertThat(receiver.capacity(), is(CAPACITY));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionForCapacityThatIsNotPowerOfTwo()
    {
        final int capacity = 777;
        final int totalBufferSize = capacity + BufferDescriptor.TRAILER_SIZE;

        when(buffer.capacity()).thenReturn(totalBufferSize);

        new Receiver(buffer);
    }

    @Test
    public void shouldNotBeLappedBeforeReception()
    {
        assertThat(receiver.lappedCount(), is(0L));
    }

    @Test
    public void shouldNotReceiveFromEmptyBuffer()
    {
        assertFalse(receiver.receiveNext());
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

        assertTrue(receiver.receiveNext());
        assertThat(receiver.type(), is(MSG_TYPE_ID));
        assertThat(receiver.buffer(), is(buffer));
        assertThat(receiver.offset(), is(msgOffset(recordOffset)));
        assertThat(receiver.length(), is(length));

        assertTrue(receiver.validate());

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

        assertTrue(receiver.receiveNext());
        assertThat(receiver.type(), is(MSG_TYPE_ID));
        assertThat(receiver.buffer(), is(buffer));
        assertThat(receiver.offset(), is(msgOffset(recordOffsetOne)));
        assertThat(receiver.length(), is(length));

        assertTrue(receiver.validate());

        assertTrue(receiver.receiveNext());
        assertThat(receiver.type(), is(MSG_TYPE_ID));
        assertThat(receiver.buffer(), is(buffer));
        assertThat(receiver.offset(), is(msgOffset(recordOffsetTwo)));
        assertThat(receiver.length(), is(length));

        assertTrue(receiver.validate());
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

        assertTrue(receiver.receiveNext());
        assertThat(receiver.type(), is(MSG_TYPE_ID));
        assertThat(receiver.buffer(), is(buffer));
        assertThat(receiver.offset(), is(msgOffset(recordOffset)));
        assertThat(receiver.length(), is(length));

        assertTrue(receiver.validate());
        assertThat(receiver.lappedCount(), is(greaterThan(0L)));
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

        assertTrue(receiver.receiveNext()); // To catch up to record before padding.

        assertTrue(receiver.receiveNext()); // no skip over the padding and read next record.
        assertThat(receiver.type(), is(MSG_TYPE_ID));
        assertThat(receiver.buffer(), is(buffer));
        assertThat(receiver.offset(), is(msgOffset(recordOffset)));
        assertThat(receiver.length(), is(length));

        assertTrue(receiver.validate());
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

        assertTrue(receiver.receiveNext());
        assertThat(receiver.type(), is(MSG_TYPE_ID));
        assertThat(receiver.buffer(), is(buffer));
        assertThat(receiver.offset(), is(msgOffset(recordOffset)));
        assertThat(receiver.length(), is(length));

        assertFalse(receiver.validate()); // Need to receiveNext() to catch up with transmission again.

        final InOrder inOrder = inOrder(buffer);
        inOrder.verify(buffer).getLongVolatile(TAIL_COUNTER_INDEX);
        inOrder.verify(buffer).getLongVolatile(tailSequenceOffset(recordOffset));
        inOrder.verify(buffer).getLongVolatile(tailSequenceOffset(recordOffset));
    }
}
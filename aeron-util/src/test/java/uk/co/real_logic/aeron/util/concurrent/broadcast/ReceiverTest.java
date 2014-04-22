package uk.co.real_logic.aeron.util.concurrent.broadcast;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReceiverTest
{
    public static final int MSG_TYPE_ID = 7;
    public static final int CAPACITY = 1024;
    public static final int TOTAL_BUFFER_SIZE = CAPACITY + BufferDescriptor.TRAILER_SIZE;
    public static final int TAIL_COUNTER_INDEX = CAPACITY + BufferDescriptor.TAIL_COUNTER_OFFSET;

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
}
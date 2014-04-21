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
package uk.co.real_logic.aeron.util.concurrent.broadcast;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransmitterTest
{
    public static final int MSG_TYPE_ID = 7;
    public static final int CAPACITY = 1024;
    public static final int TOTAL_BUFFER_SIZE = CAPACITY + BufferDescriptor.TRAILER_SIZE;
    public static final int TAIL_COUNTER_INDEX = CAPACITY + BufferDescriptor.TAIL_COUNTER_OFFSET;

    private final AtomicBuffer atomicBuffer = mock(AtomicBuffer.class);
    private Transmitter transmitter;

    @Before
    public void setUp()
    {
        when(atomicBuffer.capacity()).thenReturn(TOTAL_BUFFER_SIZE);

        transmitter = new Transmitter(atomicBuffer);
    }

    @Test
    public void shouldCalculateCapacityForBuffer()
    {
        assertThat(transmitter.capacity(), is(CAPACITY));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionForCapacityThatIsNotPowerOfTwo()
    {
        final int capacity = 777;
        final int totalBufferSize = capacity + BufferDescriptor.TRAILER_SIZE;

        when(atomicBuffer.capacity()).thenReturn(totalBufferSize);

        new Transmitter(atomicBuffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenMaxEventSizeExceeded()
    {
        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);

        transmitter.transmit(MSG_TYPE_ID, srcBuffer, 0, transmitter.maxMsgLength() + 1);
    }


    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenMessageTypeIdInvalid()
    {
        final int invalidMsgId = -1;
        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);

        transmitter.transmit(invalidMsgId, srcBuffer, 0, 32);
    }
}

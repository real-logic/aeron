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
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static java.lang.Integer.valueOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.STATE_TAIL_COUNTER_OFFSET;

public class LogBufferWriterTest
{
    private static final int LOG_BUFFER_SIZE = 1024 * 16;
    private static final int STATE_BUFFER_SIZE = 1024 * 4;

    private final AtomicBuffer logAtomicBuffer = mock(AtomicBuffer.class);
    private final AtomicBuffer stateAtomicBuffer = mock(AtomicBuffer.class);

    private LogBufferWriter writer;

    @Before
    public void setUp()
    {
        when(valueOf(logAtomicBuffer.capacity())).thenReturn(valueOf(LOG_BUFFER_SIZE));
        when(valueOf(stateAtomicBuffer.capacity())).thenReturn(valueOf(STATE_BUFFER_SIZE));

        writer = new LogBufferWriter(logAtomicBuffer, stateAtomicBuffer);
    }

    @Test
    public void shouldReportCapacity()
    {
        assertThat(valueOf(writer.capacity()), is(valueOf(LOG_BUFFER_SIZE)));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientCapacityForLog()
    {
        when(valueOf(logAtomicBuffer.capacity())).thenReturn(valueOf(LogBufferDescriptor.LOG_MIN_SIZE - 1));

        new LogBufferWriter(logAtomicBuffer, stateAtomicBuffer);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCapacityNotMultipleOfAlignment()
    {
        final int logBufferCapacity = LogBufferDescriptor.LOG_MIN_SIZE + RecordDescriptor.ALIGNMENT + 1;
        when(valueOf(logAtomicBuffer.capacity())).thenReturn(valueOf(logBufferCapacity));

        new LogBufferWriter(logAtomicBuffer, stateAtomicBuffer);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientStateBufferCapacity()
    {
        when(valueOf(stateAtomicBuffer.capacity())).thenReturn(valueOf(LogBufferDescriptor.STATE_SIZE - 1));

        new LogBufferWriter(logAtomicBuffer, stateAtomicBuffer);
    }

    @Test
    public void shouldReportCurrentTail()
    {
        final int tailValue = 64;
        when(valueOf(stateAtomicBuffer.getIntVolatile(STATE_TAIL_COUNTER_OFFSET))).thenReturn(valueOf(tailValue));

        assertThat(valueOf(writer.tail()), is(valueOf(tailValue)));
    }

    @Test
    public void shouldReportCurrentTailAtCapacity()
    {
        final int tailValue = LOG_BUFFER_SIZE + 64;
        when(valueOf(stateAtomicBuffer.getIntVolatile(STATE_TAIL_COUNTER_OFFSET))).thenReturn(valueOf(tailValue));

        assertThat(valueOf(writer.tail()), is(valueOf(LOG_BUFFER_SIZE)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenMaxMessageLengthExceeded()
    {
        final int maxMessageLength = writer.maxMessageLength();
        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);

        writer.write(srcBuffer, 0, maxMessageLength + 1);
    }
}

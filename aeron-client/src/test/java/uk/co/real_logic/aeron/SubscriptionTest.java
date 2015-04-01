/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.*;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.status.PositionReporter;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;

public class SubscriptionTest
{
    private static final String CHANNEL = "udp://localhost:40124";
    private static final int STREAM_ID_1 = 2;
    private static final int SESSION_ID_1 = 13;
    private static final int SESSION_ID_2 = 14;
    private static final int TERM_ID_1 = 1;
    private static final int ACTIVE_INDEX = LogBufferDescriptor.indexByTerm(TERM_ID_1, TERM_ID_1);
    private static final long SUBSCRIPTION_CORRELATION_ID = 100;
    private static final long CONNECTION_CORRELATION_ID = 101;
    private static final int READ_BUFFER_CAPACITY = 1024;
    public static final byte FLAGS = (byte)FrameDescriptor.UNFRAGMENTED;
    public static final int FRAGMENT_COUNT_LIMIT = Integer.MAX_VALUE;
    public static final int HEADER_LENGTH = Header.LENGTH;

    private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_CAPACITY);
    private final UnsafeBuffer atomicReadBuffer = new UnsafeBuffer(readBuffer);
    private final ClientConductor conductor = mock(ClientConductor.class);
    private final PositionReporter reporter = mock(PositionReporter.class);
    private final DataHandler dataHandler = mock(DataHandler.class);
    private final Header header = mock(Header.class);

    private Subscription subscription;
    private TermReader[] readers;
    private LogBuffers logBuffers = mock(LogBuffers.class);
    private UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);

    @Before
    public void setUp()
    {
        when(header.flags()).thenReturn(FLAGS);
        when(header.sessionId()).thenReturn(SESSION_ID_1);
        when(termBuffer.capacity()).thenReturn(TERM_MIN_LENGTH);

        readers = new TermReader[PARTITION_COUNT];
        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            readers[i] = mock(TermReader.class);
            when(readers[i].read(anyInt(), any(), anyInt())).thenReturn(0);
            when(readers[i].termBuffer()).thenReturn(termBuffer);
        }

        subscription = new Subscription(conductor, dataHandler, CHANNEL, STREAM_ID_1, SUBSCRIPTION_CORRELATION_ID);
    }

    @Test
    public void shouldReadNothingWithNoConnections()
    {
        assertThat(subscription.poll(1), is(0));
    }

    @Test
    public void shouldReadNothingWhenThereIsNoData()
    {
        onTermBuffersMapped(SESSION_ID_1);

        assertThat(subscription.poll(1), is(0));
    }

    @Test
    public void shouldReadData()
    {
        onTermBuffersMapped(SESSION_ID_1);

        when(readers[ACTIVE_INDEX].read(anyInt(), any(), anyInt())).then(
            (invocation) ->
            {
                final DataHandler handler = (DataHandler)invocation.getArguments()[1];
                handler.onData(atomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, header);

                return 1;
            });

        assertThat(subscription.poll(FRAGMENT_COUNT_LIMIT), is(1));
        verify(dataHandler).onData(
            eq(atomicReadBuffer),
            eq(HEADER_LENGTH),
            eq(READ_BUFFER_CAPACITY - HEADER_LENGTH),
            any(Header.class));
    }

    @Test
    public void shouldReadDataFromMultipleSources()
    {
        onTermBuffersMapped(SESSION_ID_1);
        onTermBuffersMapped(SESSION_ID_2);

        when(readers[ACTIVE_INDEX].read(anyInt(), any(), anyInt())).then(
            (invocation) ->
            {
                final DataHandler handler = (DataHandler)invocation.getArguments()[1];
                handler.onData(atomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, header);

                return 1;
            });

        assertThat(subscription.poll(FRAGMENT_COUNT_LIMIT), is(2));
    }

    private void onTermBuffersMapped(final int sessionId1)
    {
        subscription.onConnectionReady(sessionId1, 0, CONNECTION_CORRELATION_ID, readers, reporter, logBuffers);
    }
}

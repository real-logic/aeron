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
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class SubscriptionTest
{
    private static final String CHANNEL = "udp://localhost:40124";
    private static final int STREAM_ID_1 = 2;
    private static final long SUBSCRIPTION_CORRELATION_ID = 100;
    private static final int READ_BUFFER_CAPACITY = 1024;
    private static final byte FLAGS = (byte)FrameDescriptor.UNFRAGMENTED;
    private static final int FRAGMENT_COUNT_LIMIT = Integer.MAX_VALUE;
    private static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;

    private final UnsafeBuffer atomicReadBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(READ_BUFFER_CAPACITY));
    private final Consumer<Throwable> errorHandler = mock(Consumer.class);
    private final ClientConductor conductor = mock(ClientConductor.class);
    private final FragmentHandler fragmentHandler = mock(FragmentHandler.class);
    private final Connection connectionOneMock = mock(Connection.class);
    private final Header header = mock(Header.class);
    private final Connection connectionTwoMock = mock(Connection.class);

    private Subscription subscription;

    @Before
    public void setUp()
    {
        when(header.flags()).thenReturn(FLAGS);

        subscription = new Subscription(conductor, CHANNEL, STREAM_ID_1, SUBSCRIPTION_CORRELATION_ID, errorHandler);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldEnsureTheSubscriptionIsOpenWhenPolling()
    {
        subscription.close();
        subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
    }

    @Test
    public void shouldReadNothingWithNoConnections()
    {
        assertThat(subscription.poll(fragmentHandler, 1), is(0));
    }

    @Test
    public void shouldReadNothingWhenThereIsNoData()
    {
        subscription.addConnection(connectionOneMock);

        assertThat(subscription.poll(fragmentHandler, 1), is(0));
    }

    @Test
    public void shouldReadData()
    {
        subscription.addConnection(connectionOneMock);

        when(connectionOneMock.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT, errorHandler)).then(
            (invocation) ->
            {
                final FragmentHandler handler = (FragmentHandler)invocation.getArguments()[0];
                handler.onFragment(atomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, header);

                return 1;
            });

        assertThat(subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT), is(1));
        verify(fragmentHandler).onFragment(
            eq(atomicReadBuffer),
            eq(HEADER_LENGTH),
            eq(READ_BUFFER_CAPACITY - HEADER_LENGTH),
            any(Header.class));
    }

    @Test
    public void shouldReadDataFromMultipleSources()
    {
        subscription.addConnection(connectionOneMock);
        subscription.addConnection(connectionTwoMock);

        when(connectionOneMock.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT, errorHandler)).then(
            (invocation) ->
            {
                final FragmentHandler handler = (FragmentHandler)invocation.getArguments()[0];
                handler.onFragment(atomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, header);

                return 1;
            });

        when(connectionTwoMock.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT, errorHandler)).then(
            (invocation) ->
            {
                final FragmentHandler handler = (FragmentHandler)invocation.getArguments()[0];
                handler.onFragment(atomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, header);

                return 1;
            });


        assertThat(subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT), is(2));
    }
}

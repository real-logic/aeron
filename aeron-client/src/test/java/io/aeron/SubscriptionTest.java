/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron;

import io.aeron.logbuffer.LogBufferDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class SubscriptionTest
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:40124";
    private static final int STREAM_ID_1 = 1002;
    private static final int INITIAL_TERM_ID = 7;
    private static final long SUBSCRIPTION_CORRELATION_ID = 100;
    private static final int READ_BUFFER_CAPACITY = 1024;
    private static final int FRAGMENT_COUNT_LIMIT = Integer.MAX_VALUE;
    private static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;

    private final UnsafeBuffer atomicReadBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(READ_BUFFER_CAPACITY));
    private final ClientConductor conductor = mock(ClientConductor.class);
    private final FragmentHandler fragmentHandler = mock(FragmentHandler.class);
    private final Image imageOneMock = mock(Image.class);
    private final Image imageTwoMock = mock(Image.class);
    private final Header header = new Header(
        INITIAL_TERM_ID, LogBufferDescriptor.positionBitsToShift(LogBufferDescriptor.TERM_MIN_LENGTH));
    private final AvailableImageHandler availableImageHandlerMock = mock(AvailableImageHandler.class);
    private final UnavailableImageHandler unavailableImageHandlerMock = mock(UnavailableImageHandler.class);

    private Subscription subscription;

    @BeforeEach
    public void setUp()
    {
        when(imageOneMock.correlationId()).thenReturn(1L);
        when(imageTwoMock.correlationId()).thenReturn(2L);

        subscription = new Subscription(
            conductor,
            CHANNEL,
            STREAM_ID_1,
            SUBSCRIPTION_CORRELATION_ID,
            availableImageHandlerMock,
            unavailableImageHandlerMock);

        doAnswer(
            (invocation) ->
            {
                subscription.internalClose();
                return null;
            }).when(conductor).releaseSubscription(subscription);
    }

    @Test
    public void shouldEnsureTheSubscriptionIsOpenWhenPolling()
    {
        subscription.close();
        assertTrue(subscription.isClosed());

        verify(conductor).releaseSubscription(subscription);
    }

    @Test
    public void shouldReadNothingWhenNoImages()
    {
        assertEquals(0, subscription.poll(fragmentHandler, 1));
    }

    @Test
    public void shouldReadNothingWhenThereIsNoData()
    {
        subscription.addImage(imageOneMock);

        assertEquals(0, subscription.poll(fragmentHandler, 1));
    }

    @Test
    public void shouldReadData()
    {
        subscription.addImage(imageOneMock);

        when(imageOneMock.poll(any(FragmentHandler.class), anyInt())).then(
            (invocation) ->
            {
                final FragmentHandler handler = (FragmentHandler)invocation.getArguments()[0];
                handler.onFragment(atomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, header);

                return 1;
            });

        assertEquals(1, subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT));
        verify(fragmentHandler).onFragment(
            eq(atomicReadBuffer),
            eq(HEADER_LENGTH),
            eq(READ_BUFFER_CAPACITY - HEADER_LENGTH),
            any(Header.class));
    }

    @Test
    public void shouldReadDataFromMultipleSources()
    {
        subscription.addImage(imageOneMock);
        subscription.addImage(imageTwoMock);

        when(imageOneMock.poll(any(FragmentHandler.class), anyInt())).then(
            (invocation) ->
            {
                final FragmentHandler handler = (FragmentHandler)invocation.getArguments()[0];
                handler.onFragment(atomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, header);

                return 1;
            });

        when(imageTwoMock.poll(any(FragmentHandler.class), anyInt())).then(
            (invocation) ->
            {
                final FragmentHandler handler = (FragmentHandler)invocation.getArguments()[0];
                handler.onFragment(atomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, header);

                return 1;
            });

        assertEquals(2, subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT));
    }
}

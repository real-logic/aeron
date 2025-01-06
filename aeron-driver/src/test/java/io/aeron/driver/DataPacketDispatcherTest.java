/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.driver;

import io.aeron.driver.media.ReceiveChannelEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.SetupFlyweight;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

import static org.mockito.Mockito.*;

class DataPacketDispatcherTest
{
    private static final long CORRELATION_ID_1 = 101;
    private static final long CORRELATION_ID_2 = 102;
    private static final int STREAM_ID = 1010;
    private static final int INITIAL_TERM_ID = 3;
    private static final int ACTIVE_TERM_ID = 3;
    private static final int SESSION_ID = 1;
    private static final int TERM_OFFSET = 0;
    private static final int LENGTH = DataHeaderFlyweight.HEADER_LENGTH + 100;
    private static final int MTU_LENGTH = 1024;
    private static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final InetSocketAddress SRC_ADDRESS = new InetSocketAddress("localhost", 4510);
    private static final int STREAM_SESSION_LIMIT = 10;

    private final DriverConductorProxy mockConductorProxy = mock(DriverConductorProxy.class);
    private final Receiver mockReceiver = mock(Receiver.class);
    private final DataPacketDispatcher dispatcher = new DataPacketDispatcher(
        mockConductorProxy, mockReceiver, STREAM_SESSION_LIMIT);
    private final DataHeaderFlyweight mockHeader = mock(DataHeaderFlyweight.class);
    private final SetupFlyweight mockSetupHeader = mock(SetupFlyweight.class);
    private final UnsafeBuffer mockBuffer = mock(UnsafeBuffer.class);
    private final PublicationImage mockImage = mock(PublicationImage.class);
    private final ReceiveChannelEndpoint mockChannelEndpoint = mock(ReceiveChannelEndpoint.class);

    @BeforeEach
    void setUp()
    {
        when(mockHeader.sessionId()).thenReturn(SESSION_ID);
        when(mockHeader.streamId()).thenReturn(STREAM_ID);
        when(mockHeader.termId()).thenReturn(ACTIVE_TERM_ID);
        when(mockHeader.termOffset()).thenReturn(TERM_OFFSET);

        when(mockImage.sessionId()).thenReturn(SESSION_ID);
        when(mockImage.streamId()).thenReturn(STREAM_ID);

        when(mockSetupHeader.sessionId()).thenReturn(SESSION_ID);
        when(mockSetupHeader.streamId()).thenReturn(STREAM_ID);
        when(mockSetupHeader.activeTermId()).thenReturn(ACTIVE_TERM_ID);
        when(mockSetupHeader.initialTermId()).thenReturn(INITIAL_TERM_ID);
        when(mockSetupHeader.termOffset()).thenReturn(TERM_OFFSET);
        when(mockSetupHeader.mtuLength()).thenReturn(MTU_LENGTH);
        when(mockSetupHeader.termLength()).thenReturn(TERM_LENGTH);
    }

    @Test
    void shouldElicitSetupMessageWhenDataArrivesForSubscriptionWithoutImage()
    {
        dispatcher.addSubscription(STREAM_ID);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);

        verify(mockImage, never()).insertPacket(anyInt(), anyInt(), any(), anyInt(), anyInt(), any());
        verify(mockChannelEndpoint).sendSetupElicitingStatusMessage(0, SRC_ADDRESS, SESSION_ID, STREAM_ID);
        verify(mockReceiver).addPendingSetupMessage(SESSION_ID, STREAM_ID, 0, mockChannelEndpoint, false, SRC_ADDRESS);
    }

    @Test
    void shouldOnlyElicitSetupMessageOnceWhenDataArrivesForSubscriptionWithoutImage()
    {
        dispatcher.addSubscription(STREAM_ID);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);

        verify(mockImage, never()).insertPacket(anyInt(), anyInt(), any(), anyInt(), anyInt(), any());
        verify(mockChannelEndpoint).sendSetupElicitingStatusMessage(0, SRC_ADDRESS, SESSION_ID, STREAM_ID);
        verify(mockReceiver).addPendingSetupMessage(SESSION_ID, STREAM_ID, 0, mockChannelEndpoint, false, SRC_ADDRESS);
    }

    @Test
    void shouldElicitSetupMessageAgainWhenDataArrivesForSubscriptionWithoutImageAfterRemovePendingSetup()
    {
        dispatcher.addSubscription(STREAM_ID);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);
        dispatcher.removePendingSetup(SESSION_ID, STREAM_ID);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);

        verify(mockImage, never()).insertPacket(anyInt(), anyInt(), any(), anyInt(), anyInt(), any());
        verify(mockChannelEndpoint, times(2)).sendSetupElicitingStatusMessage(0, SRC_ADDRESS, SESSION_ID, STREAM_ID);
        verify(mockReceiver, times(2))
            .addPendingSetupMessage(SESSION_ID, STREAM_ID, 0, mockChannelEndpoint, false, SRC_ADDRESS);
    }

    @Test
    void shouldRequestCreateImageUponReceivingSetup()
    {
        dispatcher.addSubscription(STREAM_ID);
        dispatcher.onSetupMessage(mockChannelEndpoint, mockSetupHeader, SRC_ADDRESS, 0);

        verify(mockConductorProxy).createPublicationImage(
            SESSION_ID, STREAM_ID, INITIAL_TERM_ID, ACTIVE_TERM_ID, TERM_OFFSET, TERM_LENGTH,
            MTU_LENGTH, 0, (short)0, SRC_ADDRESS, SRC_ADDRESS, mockChannelEndpoint);
    }

    @Test
    void shouldOnlyRequestCreateImageOnceUponReceivingSetup()
    {
        dispatcher.addSubscription(STREAM_ID);
        dispatcher.onSetupMessage(mockChannelEndpoint, mockSetupHeader, SRC_ADDRESS, 0);
        dispatcher.onSetupMessage(mockChannelEndpoint, mockSetupHeader, SRC_ADDRESS, 0);
        dispatcher.onSetupMessage(mockChannelEndpoint, mockSetupHeader, SRC_ADDRESS, 0);

        verify(mockConductorProxy).createPublicationImage(
            SESSION_ID, STREAM_ID, INITIAL_TERM_ID, ACTIVE_TERM_ID, TERM_OFFSET, TERM_LENGTH,
            MTU_LENGTH, 0, (short)0, SRC_ADDRESS, SRC_ADDRESS, mockChannelEndpoint);
    }

    @Test
    void shouldNotRequestCreateImageOnceUponReceivingSetupAfterImageAdded()
    {
        dispatcher.addSubscription(STREAM_ID);
        dispatcher.addPublicationImage(mockImage);
        dispatcher.onSetupMessage(mockChannelEndpoint, mockSetupHeader, SRC_ADDRESS, 0);

        verifyNoInteractions(mockConductorProxy);
    }

    @Test
    void shouldSetImageInactiveOnRemoveSubscription()
    {
        dispatcher.addSubscription(STREAM_ID);
        dispatcher.addPublicationImage(mockImage);
        dispatcher.removeSubscription(STREAM_ID);

        verify(mockImage).deactivate();
    }

    @Test
    void shouldSetImageInactiveOnRemoveImage()
    {
        dispatcher.addSubscription(STREAM_ID);
        dispatcher.addPublicationImage(mockImage);
        dispatcher.removePublicationImage(mockImage);

        verify(mockImage).deactivate();
    }

    @Test
    void shouldIgnoreDataAndSetupAfterImageRemovedButHasNotEndedStream()
    {
        when(mockImage.isEndOfStream()).thenReturn(false);

        dispatcher.addSubscription(STREAM_ID);
        dispatcher.addPublicationImage(mockImage);
        dispatcher.removePublicationImage(mockImage);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);
        dispatcher.onSetupMessage(mockChannelEndpoint, mockSetupHeader, SRC_ADDRESS, 0);

        verify(mockImage, never()).insertPacket(anyInt(), anyInt(), any(), anyInt(), anyInt(), any());

        verifyNoInteractions(mockConductorProxy);
        verifyNoInteractions(mockReceiver);
    }

    @Test
    void shouldIgnoreDataButAllowSetupAfterImageRemovedWhenEndOfStreamReached()
    {
        when(mockImage.isEndOfStream()).thenReturn(true);

        dispatcher.addSubscription(STREAM_ID);
        dispatcher.addPublicationImage(mockImage);
        dispatcher.removePublicationImage(mockImage);

        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);
        verify(mockImage, never()).insertPacket(anyInt(), anyInt(), any(), anyInt(), anyInt(), any());

        dispatcher.onSetupMessage(mockChannelEndpoint, mockSetupHeader, SRC_ADDRESS, 0);
        verify(mockConductorProxy).createPublicationImage(
            anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), anyInt(),
            anyShort(), any(), any(), any());
        verify(mockReceiver).addPendingSetupMessage(anyInt(), anyInt(), anyInt(), any(), anyBoolean(), any());

        verifyNoMoreInteractions(mockConductorProxy);
        verifyNoMoreInteractions(mockReceiver);
    }

    @Test
    void shouldNotIgnoreDataAndSetupAfterImageRemovedAndCoolDownRemoved()
    {
        dispatcher.addSubscription(STREAM_ID);
        dispatcher.addPublicationImage(mockImage);
        dispatcher.removePublicationImage(mockImage);
        dispatcher.removeCoolDown(SESSION_ID, STREAM_ID);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);
        dispatcher.onSetupMessage(mockChannelEndpoint, mockSetupHeader, SRC_ADDRESS, 0);

        verify(mockImage, never()).insertPacket(anyInt(), anyInt(), any(), anyInt(), anyInt(), any());

        final InOrder inOrder = inOrder(mockChannelEndpoint, mockReceiver, mockConductorProxy);
        inOrder.verify(mockChannelEndpoint).sendSetupElicitingStatusMessage(0, SRC_ADDRESS, SESSION_ID, STREAM_ID);
        inOrder.verify(mockReceiver)
            .addPendingSetupMessage(SESSION_ID, STREAM_ID, 0, mockChannelEndpoint, false, SRC_ADDRESS);
        inOrder.verify(mockConductorProxy).createPublicationImage(
            SESSION_ID, STREAM_ID, INITIAL_TERM_ID, ACTIVE_TERM_ID, TERM_OFFSET, TERM_LENGTH,
            MTU_LENGTH, 0, (short)0, SRC_ADDRESS, SRC_ADDRESS, mockChannelEndpoint);
    }

    @Test
    void shouldDispatchDataToCorrectImage()
    {
        dispatcher.addSubscription(STREAM_ID);
        dispatcher.addPublicationImage(mockImage);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);

        verify(mockImage).activate();
        verify(mockImage).insertPacket(ACTIVE_TERM_ID, TERM_OFFSET, mockBuffer, LENGTH, 0, SRC_ADDRESS);
    }

    @Test
    void shouldNotRemoveNewPublicationImageFromOldRemovePublicationImageAfterRemoveSubscription()
    {
        final PublicationImage mockImage1 = mock(PublicationImage.class);
        final PublicationImage mockImage2 = mock(PublicationImage.class);

        when(mockImage1.sessionId()).thenReturn(SESSION_ID);
        when(mockImage1.streamId()).thenReturn(STREAM_ID);
        when(mockImage1.correlationId()).thenReturn(CORRELATION_ID_1);

        when(mockImage2.sessionId()).thenReturn(SESSION_ID);
        when(mockImage2.streamId()).thenReturn(STREAM_ID);
        when(mockImage2.correlationId()).thenReturn(CORRELATION_ID_2);

        dispatcher.addSubscription(STREAM_ID);
        dispatcher.addPublicationImage(mockImage1);
        dispatcher.removeSubscription(STREAM_ID);
        dispatcher.addSubscription(STREAM_ID);
        dispatcher.addPublicationImage(mockImage2);
        dispatcher.removePublicationImage(mockImage1);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);

        verify(mockImage1, never()).insertPacket(anyInt(), anyInt(), any(), anyInt(), anyInt(), any());
        verify(mockImage2).insertPacket(ACTIVE_TERM_ID, TERM_OFFSET, mockBuffer, LENGTH, 0, SRC_ADDRESS);
    }

    @Test
    void shouldRemoveSessionSpecificSubscriptionWithoutAny()
    {
        dispatcher.addSubscription(STREAM_ID, SESSION_ID);
        dispatcher.removeSubscription(STREAM_ID, SESSION_ID);
    }

    @Test
    void shouldRemoveSessionSpecificSubscriptionAndStillReceiveIntoImage()
    {
        final PublicationImage mockImage = mock(PublicationImage.class);

        when(mockImage.sessionId()).thenReturn(SESSION_ID);
        when(mockImage.streamId()).thenReturn(STREAM_ID);
        when(mockImage.correlationId()).thenReturn(CORRELATION_ID_1);

        dispatcher.addSubscription(STREAM_ID);
        dispatcher.addPublicationImage(mockImage);
        dispatcher.addSubscription(STREAM_ID, SESSION_ID);
        dispatcher.removeSubscription(STREAM_ID, SESSION_ID);
        dispatcher.onDataPacket(mockChannelEndpoint, mockHeader, mockBuffer, LENGTH, SRC_ADDRESS, 0);

        verify(mockImage).insertPacket(ACTIVE_TERM_ID, TERM_OFFSET, mockBuffer, LENGTH, 0, SRC_ADDRESS);
    }

    @Test
    void shouldPreventNewSessionsOnceStreamSessionLimitIsExceeded()
    {
        final PublicationImage mockImage = mock(PublicationImage.class);

        when(mockImage.streamId()).thenReturn(STREAM_ID);
        when(mockImage.correlationId()).thenReturn(CORRELATION_ID_1);

        dispatcher.addSubscription(STREAM_ID);

        int sessionId = SESSION_ID;
        for (int i = 0; i < STREAM_SESSION_LIMIT; i++)
        {
            when(mockImage.sessionId()).thenReturn(sessionId);
            when(mockSetupHeader.sessionId()).thenReturn(sessionId);
            sessionId++;

            dispatcher.onSetupMessage(mockChannelEndpoint, mockSetupHeader, SRC_ADDRESS, 0);
            dispatcher.addPublicationImage(mockImage);
        }
        verify(mockConductorProxy, times(10)).createPublicationImage(
            anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), anyInt(),
            anyShort(), any(), any(), any());

        when(mockSetupHeader.sessionId()).thenReturn(sessionId);
        try
        {
            dispatcher.onSetupMessage(mockChannelEndpoint, mockSetupHeader, SRC_ADDRESS, 0);
        }
        catch (final Exception ignore)
        {
        }
        verifyNoMoreInteractions(mockConductorProxy);
    }
}

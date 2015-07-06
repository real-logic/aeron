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
package uk.co.real_logic.aeron.driver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.driver.media.*;
import uk.co.real_logic.aeron.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.driver.event.EventLogger;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.protocol.StatusMessageFlyweight;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class SelectorAndTransportTest
{
    private static final int RCV_PORT = 40123;
    private static final int SRC_PORT = 40124;
    private static final int SESSION_ID = 0xdeadbeef;
    private static final int STREAM_ID = 0x44332211;
    private static final int TERM_ID = 0x99887766;
    private static final int FRAME_LENGTH = 24;

    private static final UdpChannel SRC_DST = UdpChannel.parse("udp://localhost:" + SRC_PORT + "@localhost:" + RCV_PORT);
    private static final UdpChannel RCV_DST = UdpChannel.parse("udp://localhost:" + RCV_PORT);

    private static final LossGenerator NO_LOSS = (address, header, length) -> false;

    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(256);
    private final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

    private final DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

    private final InetSocketAddress rcvRemoteAddress = new InetSocketAddress("localhost", SRC_PORT);
    private final InetSocketAddress srcRemoteAddress = new InetSocketAddress("localhost", RCV_PORT);

    private final EventLogger mockTransportLogger = mock(EventLogger.class);
    private final SystemCounters mockSystemCounters = mock(SystemCounters.class);
    private final AtomicCounter mockStatusMessagesReceivedCounter = mock(AtomicCounter.class);

    private final DataPacketDispatcher mockDispatcher = mock(DataPacketDispatcher.class);
    private final NetworkPublication mockPublication = mock(NetworkPublication.class);

    private TransportPoller transportPoller;
    private SendChannelEndpoint sendChannelEndpoint;
    private ReceiveChannelEndpoint receiveChannelEndpoint;

    @Before
    public void setup()
    {
        when(mockSystemCounters.statusMessagesReceived()).thenReturn(mockStatusMessagesReceivedCounter);
        when(mockPublication.streamId()).thenReturn(STREAM_ID);
        when(mockPublication.sessionId()).thenReturn(SESSION_ID);
    }

    @After
    public void tearDown()
    {
        try
        {
            if (null != sendChannelEndpoint)
            {
                sendChannelEndpoint.close();
                processLoop(transportPoller, 5);
            }

            if (null != receiveChannelEndpoint)
            {
                receiveChannelEndpoint.close();
                processLoop(transportPoller, 5);
            }

            if (null != transportPoller)
            {
                transportPoller.close();
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    @Test(timeout = 1000)
    public void shouldHandleBasicSetupAndTearDown() throws Exception
    {
        transportPoller = new TransportPoller();
        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            RCV_DST, mockDispatcher, mockTransportLogger, mockSystemCounters, NO_LOSS);
        sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockTransportLogger, NO_LOSS, mockSystemCounters);

        receiveChannelEndpoint.openDatagramChannel();
        receiveChannelEndpoint.registerForRead(transportPoller);
        sendChannelEndpoint.openDatagramChannel();
        sendChannelEndpoint.registerForRead(transportPoller);

        processLoop(transportPoller, 5);
    }

    @Test(timeout = 1000)
    public void shouldSendEmptyDataFrameUnicastFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersReceived = new AtomicInteger(0);

        doAnswer(
            (invocation) ->
            {
                dataHeadersReceived.incrementAndGet();
                return null;
            })
            .when(mockDispatcher).onDataPacket(
                any(ReceiveChannelEndpoint.class),
                any(DataHeaderFlyweight.class),
                any(UnsafeBuffer.class),
                anyInt(),
                any(InetSocketAddress.class));

        transportPoller = new TransportPoller();
        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            RCV_DST, mockDispatcher, mockTransportLogger, mockSystemCounters, NO_LOSS);
        sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockTransportLogger, NO_LOSS, mockSystemCounters);

        receiveChannelEndpoint.openDatagramChannel();
        receiveChannelEndpoint.registerForRead(transportPoller);
        sendChannelEndpoint.openDatagramChannel();
        sendChannelEndpoint.registerForRead(transportPoller);

        encodeDataHeader.wrap(buffer, 0);
        encodeDataHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(FRAME_LENGTH);
        encodeDataHeader
            .sessionId(SESSION_ID)
            .streamId(STREAM_ID)
            .termId(TERM_ID);
        byteBuffer.position(0).limit(FRAME_LENGTH);

        processLoop(transportPoller, 5);
        sendChannelEndpoint.sendTo(byteBuffer, srcRemoteAddress);
        while (dataHeadersReceived.get() < 1)
        {
            processLoop(transportPoller, 1);
        }

        assertThat(dataHeadersReceived.get(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldSendMultipleDataFramesPerDatagramUnicastFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersReceived = new AtomicInteger(0);

        doAnswer(
            (invocation) ->
            {
                dataHeadersReceived.incrementAndGet();
                return null;
            })
            .when(mockDispatcher).onDataPacket(
                any(ReceiveChannelEndpoint.class),
                any(DataHeaderFlyweight.class),
                any(UnsafeBuffer.class),
                anyInt(),
                any(InetSocketAddress.class));

        transportPoller = new TransportPoller();
        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            RCV_DST, mockDispatcher, mockTransportLogger, mockSystemCounters, NO_LOSS);
        sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockTransportLogger, NO_LOSS, mockSystemCounters);

        receiveChannelEndpoint.openDatagramChannel();
        receiveChannelEndpoint.registerForRead(transportPoller);
        sendChannelEndpoint.openDatagramChannel();
        sendChannelEndpoint.registerForRead(transportPoller);

        encodeDataHeader.wrap(buffer, 0);
        encodeDataHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(FRAME_LENGTH);
        encodeDataHeader
            .sessionId(SESSION_ID)
            .streamId(STREAM_ID)
            .termId(TERM_ID);

        encodeDataHeader.wrap(buffer, BitUtil.align(FRAME_LENGTH, FrameDescriptor.FRAME_ALIGNMENT));
        encodeDataHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(24);
        encodeDataHeader
            .sessionId(SESSION_ID)
            .streamId(STREAM_ID)
            .termId(TERM_ID);

        byteBuffer.position(0).limit(2 * BitUtil.align(FRAME_LENGTH, FrameDescriptor.FRAME_ALIGNMENT));

        processLoop(transportPoller, 5);
        sendChannelEndpoint.sendTo(byteBuffer, srcRemoteAddress);
        while (dataHeadersReceived.get() < 1)
        {
            processLoop(transportPoller, 1);
        }

        assertThat(dataHeadersReceived.get(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldHandleSmFrameFromReceiverToSender() throws Exception
    {
        transportPoller = new TransportPoller();
        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            RCV_DST, mockDispatcher, mockTransportLogger, mockSystemCounters, NO_LOSS);
        sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockTransportLogger, NO_LOSS, mockSystemCounters);
        sendChannelEndpoint.addToDispatcher(mockPublication);

        receiveChannelEndpoint.openDatagramChannel();
        receiveChannelEndpoint.registerForRead(transportPoller);
        sendChannelEndpoint.openDatagramChannel();
        sendChannelEndpoint.registerForRead(transportPoller);

        statusMessage.wrap(buffer, 0);
        statusMessage
            .streamId(STREAM_ID)
            .sessionId(SESSION_ID)
            .consumptionTermId(TERM_ID)
            .receiverWindowLength(1000)
            .consumptionTermOffset(0)
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags((short)0)
            .headerType(HeaderFlyweight.HDR_TYPE_SM)
            .frameLength(StatusMessageFlyweight.HEADER_LENGTH);
        byteBuffer.position(0).limit(statusMessage.frameLength());

        processLoop(transportPoller, 5);
        receiveChannelEndpoint.sendTo(byteBuffer, rcvRemoteAddress);

        processLoop(transportPoller, 3);

        verify(mockStatusMessagesReceivedCounter, times(1)).orderedIncrement();
    }

    private void processLoop(final TransportPoller transportPoller, final int iterations) throws Exception
    {
        for (int i = 0; i < iterations; i++)
        {
            transportPoller.pollTransports();
        }
    }
}

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
import org.junit.Test;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

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

    private static final LossGenerator NO_LOSS = (address, length) -> false;

    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(256);
    private final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

    private final DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

    private final InetSocketAddress rcvRemoteAddress = new InetSocketAddress("localhost", SRC_PORT);
    private final InetSocketAddress srcRemoteAddress = new InetSocketAddress("localhost", RCV_PORT);

    private final EventLogger mockTransportLogger = mock(EventLogger.class);

    private final DataPacketHandler mockDataPacketHandler = mock(DataPacketHandler.class);
    private final SetupMessageHandler mockSetupMessageHandler = mock(SetupMessageHandler.class);
    private final NakFrameHandler mockNakFrameHandler = mock(NakFrameHandler.class);
    private final StatusMessageFrameHandler mockStatusMessageFrameHandler = mock(StatusMessageFrameHandler.class);

    private TransportPoller transportPoller;
    private SenderUdpChannelTransport senderTransport;
    private ReceiverUdpChannelTransport receiverTransport;

    @After
    public void tearDown()
    {
        try
        {
            if (null != senderTransport)
            {
                senderTransport.close();
                processLoop(transportPoller, 5);
            }

            if (null != receiverTransport)
            {
                receiverTransport.close();
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
    public void shouldHandleBasicSetupAndTeardown() throws Exception
    {
        transportPoller = new TransportPoller();
        receiverTransport = new ReceiverUdpChannelTransport(
            RCV_DST, mockDataPacketHandler, mockSetupMessageHandler, mockTransportLogger, NO_LOSS);
        senderTransport = new SenderUdpChannelTransport(
            SRC_DST, mockStatusMessageFrameHandler, mockNakFrameHandler, mockTransportLogger, NO_LOSS);

        receiverTransport.registerForRead(transportPoller);
        senderTransport.registerForRead(transportPoller);

        processLoop(transportPoller, 5);
    }

    @Test(timeout = 1000)
    public void shouldSendEmptyDataFrameUnicastFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersReceived = new AtomicInteger(0);
        final DataPacketHandler dataPacketHandler =
            (header, buffer, length, srcAddress) ->
            {
                assertThat(header.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
                assertThat(header.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
                assertThat(header.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(header.frameLength(), is(FRAME_LENGTH));
                assertThat(header.sessionId(), is(SESSION_ID));
                assertThat(header.streamId(), is(STREAM_ID));
                assertThat(header.termId(), is(TERM_ID));
                assertThat(header.dataOffset(), is(FRAME_LENGTH));
                dataHeadersReceived.incrementAndGet();

                return length;
            };

        transportPoller = new TransportPoller();
        receiverTransport = new ReceiverUdpChannelTransport(
            RCV_DST, dataPacketHandler, mockSetupMessageHandler, mockTransportLogger, NO_LOSS);
        senderTransport = new SenderUdpChannelTransport(
            SRC_DST, mockStatusMessageFrameHandler, mockNakFrameHandler, mockTransportLogger, NO_LOSS);

        receiverTransport.registerForRead(transportPoller);
        senderTransport.registerForRead(transportPoller);

        encodeDataHeader.wrap(buffer, 0);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                        .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(FRAME_LENGTH);
        encodeDataHeader.sessionId(SESSION_ID)
                        .streamId(STREAM_ID)
                        .termId(TERM_ID);
        byteBuffer.position(0).limit(FRAME_LENGTH);

        processLoop(transportPoller, 5);
        senderTransport.sendTo(byteBuffer, srcRemoteAddress);
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

        final DataPacketHandler dataPacketHandler =
            (header, buffer, length, srcAddress) ->
            {
                assertThat(header.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
                assertThat(header.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
                assertThat(header.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(header.frameLength(), is(FRAME_LENGTH));
                assertThat(header.sessionId(), is(SESSION_ID));
                assertThat(header.streamId(), is(STREAM_ID));
                assertThat(header.termId(), is(TERM_ID));
                assertThat(length, is(2 * BitUtil.align(FRAME_LENGTH, FrameDescriptor.FRAME_ALIGNMENT)));
                dataHeadersReceived.incrementAndGet();

                return length;
            };

        transportPoller = new TransportPoller();
        receiverTransport = new ReceiverUdpChannelTransport(
            RCV_DST, dataPacketHandler, mockSetupMessageHandler, mockTransportLogger, NO_LOSS);
        senderTransport = new SenderUdpChannelTransport(
            SRC_DST, mockStatusMessageFrameHandler, mockNakFrameHandler, mockTransportLogger, NO_LOSS);

        receiverTransport.registerForRead(transportPoller);
        senderTransport.registerForRead(transportPoller);

        encodeDataHeader.wrap(buffer, 0);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                        .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(FRAME_LENGTH);
        encodeDataHeader.sessionId(SESSION_ID)
                        .streamId(STREAM_ID)
                        .termId(TERM_ID);

        encodeDataHeader.wrap(buffer, BitUtil.align(FRAME_LENGTH, FrameDescriptor.FRAME_ALIGNMENT));
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                        .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(24);
        encodeDataHeader.sessionId(SESSION_ID)
                        .streamId(STREAM_ID)
                        .termId(TERM_ID);

        byteBuffer.position(0).limit(2 * BitUtil.align(FRAME_LENGTH, FrameDescriptor.FRAME_ALIGNMENT));

        processLoop(transportPoller, 5);
        senderTransport.sendTo(byteBuffer, srcRemoteAddress);
        while (dataHeadersReceived.get() < 1)
        {
            processLoop(transportPoller, 1);
        }

        assertThat(dataHeadersReceived.get(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldHandleSmFrameFromReceiverToSender() throws Exception
    {
        final AtomicInteger controlHeadersReceived = new AtomicInteger(0);
        final StatusMessageFrameHandler statusMessageFrameHandler =
            (header, buffer, length, srcAddress) ->
            {
                assertThat(header.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
                assertThat(header.frameLength(), is(StatusMessageFlyweight.HEADER_LENGTH));
                controlHeadersReceived.incrementAndGet();
            };

        transportPoller = new TransportPoller();
        receiverTransport = new ReceiverUdpChannelTransport(
            RCV_DST, mockDataPacketHandler, mockSetupMessageHandler, mockTransportLogger, NO_LOSS);
        senderTransport = new SenderUdpChannelTransport(
            SRC_DST, statusMessageFrameHandler, mockNakFrameHandler, mockTransportLogger, NO_LOSS);

        receiverTransport.registerForRead(transportPoller);
        senderTransport.registerForRead(transportPoller);

        statusMessage.wrap(buffer, 0);
        statusMessage.streamId(STREAM_ID)
                     .sessionId(SESSION_ID)
                     .termId(TERM_ID)
                     .receiverWindowLength(1000)
                     .completedTermOffset(0)
                     .version(HeaderFlyweight.CURRENT_VERSION)
                     .flags((short)0)
                     .headerType(HeaderFlyweight.HDR_TYPE_SM)
                     .frameLength(StatusMessageFlyweight.HEADER_LENGTH);
        byteBuffer.position(0).limit(statusMessage.frameLength());

        processLoop(transportPoller, 5);
        receiverTransport.sendTo(byteBuffer, rcvRemoteAddress);
        while (controlHeadersReceived.get() < 1)
        {
            processLoop(transportPoller, 1);
        }

        assertThat(controlHeadersReceived.get(), is(1));
    }

    private void processLoop(final TransportPoller transportPoller, final int iterations) throws Exception
    {
        for (int i = 0; i < iterations; i++)
        {
            transportPoller.pollTransports();
        }
    }
}

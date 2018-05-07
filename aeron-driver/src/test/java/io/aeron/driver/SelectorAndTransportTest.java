/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.driver;

import io.aeron.driver.media.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.BitUtil;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.UnsafeBuffer;

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

    private static final UdpChannel SRC_DST =
        UdpChannel.parse("aeron:udp?interface=localhost:" + SRC_PORT + "|endpoint=localhost:" + RCV_PORT);
    private static final UdpChannel RCV_DST = UdpChannel.parse("aeron:udp?endpoint=localhost:" + RCV_PORT);

    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(256);
    private final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

    private final DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

    private final InetSocketAddress rcvRemoteAddress = new InetSocketAddress("localhost", SRC_PORT);

    private final SystemCounters mockSystemCounters = mock(SystemCounters.class);
    private final AtomicCounter mockStatusMessagesReceivedCounter = mock(AtomicCounter.class);
    private final AtomicCounter mockSendStatusIndicator = mock(AtomicCounter.class);
    private final AtomicCounter mockReceiveStatusIndicator = mock(AtomicCounter.class);

    private final DataPacketDispatcher mockDispatcher = mock(DataPacketDispatcher.class);
    private final NetworkPublication mockPublication = mock(NetworkPublication.class);

    private DataTransportPoller dataTransportPoller = new DataTransportPoller();
    private ControlTransportPoller controlTransportPoller = new ControlTransportPoller();
    private SendChannelEndpoint sendChannelEndpoint;
    private ReceiveChannelEndpoint receiveChannelEndpoint;

    private MediaDriver.Context context = new MediaDriver.Context();

    @Before
    public void setup()
    {
        when(mockSystemCounters.get(any())).thenReturn(mockStatusMessagesReceivedCounter);
        when(mockPublication.streamId()).thenReturn(STREAM_ID);
        when(mockPublication.sessionId()).thenReturn(SESSION_ID);

        context.systemCounters(mockSystemCounters);
        context.receiveChannelEndpointThreadLocals(new ReceiveChannelEndpointThreadLocals(context));
    }

    @After
    public void tearDown()
    {
        try
        {
            if (null != sendChannelEndpoint)
            {
                sendChannelEndpoint.close();
                processLoop(controlTransportPoller, 5);
            }

            if (null != receiveChannelEndpoint)
            {
                receiveChannelEndpoint.close();
                processLoop(dataTransportPoller, 5);
            }

            if (null != dataTransportPoller)
            {
                dataTransportPoller.close();
                controlTransportPoller.close();
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    @Test(timeout = 1000)
    public void shouldHandleBasicSetupAndTearDown()
    {
        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            RCV_DST, mockDispatcher, mockReceiveStatusIndicator, context);
        sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockSendStatusIndicator, context);

        receiveChannelEndpoint.openDatagramChannel(mockReceiveStatusIndicator);
        receiveChannelEndpoint.registerForRead(dataTransportPoller);
        sendChannelEndpoint.openDatagramChannel(mockSendStatusIndicator);
        sendChannelEndpoint.registerForRead(controlTransportPoller);

        processLoop(dataTransportPoller, 5);
    }

    @Test(timeout = 1000)
    public void shouldSendEmptyDataFrameUnicastFromSourceToReceiver()
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
            any(InetSocketAddress.class),
            anyInt());

        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            RCV_DST, mockDispatcher, mockReceiveStatusIndicator, context);
        sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockSendStatusIndicator, context);

        receiveChannelEndpoint.openDatagramChannel(mockReceiveStatusIndicator);
        receiveChannelEndpoint.registerForRead(dataTransportPoller);
        sendChannelEndpoint.openDatagramChannel(mockSendStatusIndicator);
        sendChannelEndpoint.registerForRead(controlTransportPoller);

        encodeDataHeader.wrap(buffer);
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

        processLoop(dataTransportPoller, 5);
        sendChannelEndpoint.send(byteBuffer);
        while (dataHeadersReceived.get() < 1)
        {
            processLoop(dataTransportPoller, 1);
        }

        assertThat(dataHeadersReceived.get(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldSendMultipleDataFramesPerDatagramUnicastFromSourceToReceiver()
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
            any(InetSocketAddress.class),
            anyInt());

        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            RCV_DST, mockDispatcher, mockReceiveStatusIndicator, context);
        sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockSendStatusIndicator, context);

        receiveChannelEndpoint.openDatagramChannel(mockReceiveStatusIndicator);
        receiveChannelEndpoint.registerForRead(dataTransportPoller);
        sendChannelEndpoint.openDatagramChannel(mockSendStatusIndicator);
        sendChannelEndpoint.registerForRead(controlTransportPoller);

        encodeDataHeader.wrap(buffer);
        encodeDataHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(FRAME_LENGTH);
        encodeDataHeader
            .sessionId(SESSION_ID)
            .streamId(STREAM_ID)
            .termId(TERM_ID);

        final int alignedFrameLength = BitUtil.align(FRAME_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
        encodeDataHeader.wrap(buffer, alignedFrameLength, buffer.capacity() - alignedFrameLength);
        encodeDataHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(24);
        encodeDataHeader
            .sessionId(SESSION_ID)
            .streamId(STREAM_ID)
            .termId(TERM_ID);

        byteBuffer.position(0).limit(2 * alignedFrameLength);

        processLoop(dataTransportPoller, 5);
        sendChannelEndpoint.send(byteBuffer);
        while (dataHeadersReceived.get() < 1)
        {
            processLoop(dataTransportPoller, 1);
        }

        assertThat(dataHeadersReceived.get(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldHandleSmFrameFromReceiverToSender()
    {
        final AtomicInteger controlMessagesReceived = new AtomicInteger(0);

        doAnswer(
            (invocation) ->
            {
                controlMessagesReceived.incrementAndGet();
                return null;
            })
            .when(mockPublication).onStatusMessage(any(), any());

        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            RCV_DST, mockDispatcher, mockReceiveStatusIndicator, context);
        sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockSendStatusIndicator, context);
        sendChannelEndpoint.registerForSend(mockPublication);

        receiveChannelEndpoint.openDatagramChannel(mockReceiveStatusIndicator);
        receiveChannelEndpoint.registerForRead(dataTransportPoller);
        sendChannelEndpoint.openDatagramChannel(mockSendStatusIndicator);
        sendChannelEndpoint.registerForRead(controlTransportPoller);

        statusMessage.wrap(buffer);
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

        processLoop(dataTransportPoller, 5);
        receiveChannelEndpoint.sendTo(byteBuffer, rcvRemoteAddress);

        while (controlMessagesReceived.get() < 1)
        {
            processLoop(controlTransportPoller, 1);
        }

        verify(mockStatusMessagesReceivedCounter, times(1)).incrementOrdered();
    }

    private void processLoop(final UdpTransportPoller transportPoller, final int iterations)
    {
        for (int i = 0; i < iterations; i++)
        {
            transportPoller.pollTransports();
        }
    }
}

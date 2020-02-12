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
package io.aeron.driver;

import io.aeron.driver.media.*;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.BitUtil;
import org.agrona.ErrorHandler;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocateDirect;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
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

    private final ByteBuffer byteBuffer = allocateDirect(256);
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
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);

    private final DataTransportPoller dataTransportPoller =
        new DataTransportPoller(allocateDirect(Configuration.MAX_UDP_PAYLOAD_LENGTH), errorHandler);
    private final ControlTransportPoller controlTransportPoller =
        new ControlTransportPoller(allocateDirect(Configuration.MAX_UDP_PAYLOAD_LENGTH), errorHandler);
    private SendChannelEndpoint sendChannelEndpoint;
    private ReceiveChannelEndpoint receiveChannelEndpoint;

    private final MediaDriver.Context context = new MediaDriver.Context();

    @BeforeEach
    public void setup()
    {
        when(mockSystemCounters.get(any())).thenReturn(mockStatusMessagesReceivedCounter);
        when(mockPublication.streamId()).thenReturn(STREAM_ID);
        when(mockPublication.sessionId()).thenReturn(SESSION_ID);

        context
            .applicationSpecificFeedback(Configuration.applicationSpecificFeedback())
            .systemCounters(mockSystemCounters)
            .receiveChannelEndpointThreadLocals(new ReceiveChannelEndpointThreadLocals(context));
    }

    @AfterEach
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

            dataTransportPoller.close();
            controlTransportPoller.close();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    @Test
    public void shouldHandleBasicSetupAndTearDown()
    {
        assertTimeoutPreemptively(ofSeconds(1), () ->
        {
            receiveChannelEndpoint = new ReceiveChannelEndpoint(
                RCV_DST, mockDispatcher, mockReceiveStatusIndicator, context);
            sendChannelEndpoint = new SendChannelEndpoint(SRC_DST, mockSendStatusIndicator, context);

            receiveChannelEndpoint.openDatagramChannel(mockReceiveStatusIndicator);
            receiveChannelEndpoint.registerForRead(dataTransportPoller);
            sendChannelEndpoint.openDatagramChannel(mockSendStatusIndicator);
            sendChannelEndpoint.registerForRead(controlTransportPoller);

            processLoop(dataTransportPoller, 5);
        });
    }

    @Test
    public void shouldSendEmptyDataFrameUnicastFromSourceToReceiver()
    {
        assertTimeoutPreemptively(ofSeconds(1), () ->
        {
            final MutableInteger dataHeadersReceived = new MutableInteger(0);

            doAnswer(
                (invocation) ->
                {
                    dataHeadersReceived.value++;
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

            assertEquals(1, dataHeadersReceived.get());
        });
    }

    @Test
    public void shouldSendMultipleDataFramesPerDatagramUnicastFromSourceToReceiver()
    {
        assertTimeoutPreemptively(ofSeconds(1), () ->
        {
            final MutableInteger dataHeadersReceived = new MutableInteger(0);

            doAnswer(
                (invocation) ->
                {
                    dataHeadersReceived.value++;
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

            assertEquals(1, dataHeadersReceived.get());
        });
    }

    @Test
    public void shouldHandleSmFrameFromReceiverToSender()
    {
        assertTimeoutPreemptively(ofSeconds(1), () ->
        {
            final MutableInteger controlMessagesReceived = new MutableInteger(0);

            doAnswer(
                (invocation) ->
                {
                    controlMessagesReceived.value++;
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
        });
    }

    private void processLoop(final UdpTransportPoller transportPoller, final int iterations)
    {
        for (int i = 0; i < iterations; i++)
        {
            transportPoller.pollTransports();
        }
    }
}

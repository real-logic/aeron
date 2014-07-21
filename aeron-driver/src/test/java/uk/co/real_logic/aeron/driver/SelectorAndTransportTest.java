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
package uk.co.real_logic.aeron.driver;

import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
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
    private static final long SESSION_ID = 0xdeadbeefL;
    private static final long CHANNEL_ID = 0x44332211L;
    private static final long TERM_ID = 0x99887766L;

    private static final UdpDestination SRC_DEST =
            UdpDestination.parse("udp://localhost:" + SRC_PORT + "@localhost:" + RCV_PORT);

    private static final UdpDestination RCV_DEST =
            UdpDestination.parse("udp://localhost:" + RCV_PORT);

    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(256);
    private final AtomicBuffer buffer = new AtomicBuffer(byteBuffer);

    private final DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

    private final InetSocketAddress rcvRemoteAddress = new InetSocketAddress("localhost", SRC_PORT);
    private final InetSocketAddress rcvLocalAddress = new InetSocketAddress(RCV_PORT);
    private final InetSocketAddress srcLocalAddress = new InetSocketAddress(SRC_PORT);
    private final InetSocketAddress srcRemoteAddress = new InetSocketAddress("localhost", RCV_PORT);

    private final EventLogger mockTransportLogger = new EventLogger();
    private final EventLogger mockSelectorLogger = new EventLogger();

    private final UdpTransport.DataFrameHandler mockDataFrameHandler =
        mock(UdpTransport.DataFrameHandler.class);

    private final UdpTransport.StatusMessageFrameHandler mockStatusMessageFrameHandler =
        mock(UdpTransport.StatusMessageFrameHandler.class);

    private final UdpTransport.NakFrameHandler mockNakFrameHandler =
        mock(UdpTransport.NakFrameHandler.class);

    NioSelector nioSelector;
    UdpTransport src;
    UdpTransport rcv;

    @After
    public void tearDown()
    {
        try
        {
            if (null != src)
            {
                src.close();
                processLoop(nioSelector, 5);
            }

            if (null != rcv)
            {
                rcv.close();
                processLoop(nioSelector, 5);
            }

            if (null != nioSelector)
            {
                nioSelector.close();
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
        nioSelector = new NioSelector(mockSelectorLogger);
        rcv = new UdpTransport(RCV_DEST, mockDataFrameHandler, mockTransportLogger);
        src = new UdpTransport(SRC_DEST, mockStatusMessageFrameHandler, mockNakFrameHandler, mockTransportLogger);

        rcv.registerForRead(nioSelector);
        src.registerForRead(nioSelector);

        processLoop(nioSelector, 5);
    }

    @Test(timeout = 1000)
    public void shouldSendEmptyDataFrameUnicastFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersReceived = new AtomicInteger(0);
        final UdpTransport.DataFrameHandler dataFrameHandler = (header, buffer, length, srcAddress) ->
        {
            assertThat(header.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
            assertThat(header.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
            assertThat(header.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
            assertThat(header.frameLength(), is(24));
            assertThat(header.sessionId(), is(SESSION_ID));
            assertThat(header.channelId(), is(CHANNEL_ID));
            assertThat(header.termId(), is(TERM_ID));
            assertThat(header.dataOffset(), is(24));
            dataHeadersReceived.incrementAndGet();
        };

        nioSelector = new NioSelector(mockSelectorLogger);
        rcv = new UdpTransport(RCV_DEST, dataFrameHandler, mockTransportLogger);
        src = new UdpTransport(SRC_DEST, mockStatusMessageFrameHandler, mockNakFrameHandler, mockTransportLogger);

        rcv.registerForRead(nioSelector);
        src.registerForRead(nioSelector);

        encodeDataHeader.wrap(buffer, 0);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                        .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(24);
        encodeDataHeader.sessionId(SESSION_ID)
                        .channelId(CHANNEL_ID)
                        .termId(TERM_ID);
        byteBuffer.position(0).limit(24);

        processLoop(nioSelector, 5);
        src.sendTo(byteBuffer, srcRemoteAddress);
        while (dataHeadersReceived.get() < 1)
        {
            processLoop(nioSelector, 1);
        }

        assertThat(dataHeadersReceived.get(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldSendMultipleDataFramesPerDatagramUnicastFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersReceived = new AtomicInteger(0);

        final UdpTransport.DataFrameHandler dataFrameHandler = (header, buffer, length, srcAddress) ->
        {
            assertThat(header.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
            assertThat(header.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
            assertThat(header.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
            assertThat(header.frameLength(), is(24));
            assertThat(header.sessionId(), is(SESSION_ID));
            assertThat(header.channelId(), is(CHANNEL_ID));
            assertThat(header.termId(), is(TERM_ID));
            assertThat(length, is(FrameDescriptor.FRAME_ALIGNMENT + 24));
            dataHeadersReceived.incrementAndGet();
        };

        nioSelector = new NioSelector(mockSelectorLogger);
        rcv = new UdpTransport(RCV_DEST, dataFrameHandler, mockTransportLogger);
        src = new UdpTransport(SRC_DEST, mockStatusMessageFrameHandler, mockNakFrameHandler, mockTransportLogger);

        rcv.registerForRead(nioSelector);
        src.registerForRead(nioSelector);

        encodeDataHeader.wrap(buffer, 0);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                        .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(24);
        encodeDataHeader.sessionId(SESSION_ID)
                        .channelId(CHANNEL_ID)
                        .termId(TERM_ID);
        encodeDataHeader.wrap(buffer, FrameDescriptor.FRAME_ALIGNMENT);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                        .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(24);
        encodeDataHeader.sessionId(SESSION_ID)
                        .channelId(CHANNEL_ID)
                        .termId(TERM_ID);
        byteBuffer.position(0).limit(FrameDescriptor.FRAME_ALIGNMENT + 24);

        processLoop(nioSelector, 5);
        src.sendTo(byteBuffer, srcRemoteAddress);
        while (dataHeadersReceived.get() < 1)
        {
            processLoop(nioSelector, 1);
        }

        assertThat(dataHeadersReceived.get(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldHandleSmFrameFromReceiverToSender() throws Exception
    {
        final AtomicInteger cntlHeadersReceived = new AtomicInteger(0);
        final UdpTransport.StatusMessageFrameHandler statusMessageFrameHandler = (header, buffer, length, srcAddress) ->
        {
            assertThat(header.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
            assertThat(header.frameLength(), is(StatusMessageFlyweight.HEADER_LENGTH));
            cntlHeadersReceived.incrementAndGet();
        };

        nioSelector = new NioSelector(mockSelectorLogger);
        rcv = new UdpTransport(RCV_DEST, mockDataFrameHandler, mockTransportLogger);
        src = new UdpTransport(SRC_DEST, statusMessageFrameHandler, mockNakFrameHandler, mockTransportLogger);

        rcv.registerForRead(nioSelector);
        src.registerForRead(nioSelector);

        statusMessage.wrap(buffer, 0);
        statusMessage.channelId(CHANNEL_ID)
                     .sessionId(SESSION_ID)
                     .termId(TERM_ID)
                     .receiverWindow(1000)
                     .highestContiguousTermOffset(0)
                     .version(HeaderFlyweight.CURRENT_VERSION)
                     .flags((short) 0)
                     .headerType(HeaderFlyweight.HDR_TYPE_SM)
                     .frameLength(StatusMessageFlyweight.HEADER_LENGTH);
        byteBuffer.position(0).limit(statusMessage.frameLength());

        processLoop(nioSelector, 5);
        rcv.sendTo(byteBuffer, rcvRemoteAddress);
        while (cntlHeadersReceived.get() < 1)
        {
            processLoop(nioSelector, 1);
        }

        assertThat(cntlHeadersReceived.get(), is(1));
    }

    private void processLoop(final NioSelector nioSelector, final int iterations) throws Exception
    {
        for (int i = 0; i < iterations; i++)
        {
            nioSelector.processKeys();
        }
    }
}

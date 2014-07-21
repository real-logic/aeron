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

import org.junit.Test;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.*;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SelectorTest
{
    private static final int RCV_PORT = 40123;
    private static final int SRC_PORT = 40124;
    private static final long SESSION_ID = 0xdeadbeefL;
    private static final long CHANNEL_ID = 0x44332211L;
    private static final long TERM_ID = 0x99887766L;

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(256);
    private final AtomicBuffer atomicBuffer = new AtomicBuffer(buffer);
    private final DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();
    private final InetSocketAddress rcvRemoteAddress = new InetSocketAddress("localhost", SRC_PORT);
    private final InetSocketAddress rcvLocalAddress = new InetSocketAddress(RCV_PORT);
    private final InetSocketAddress srcLocalAddress = new InetSocketAddress(SRC_PORT);
    private final InetSocketAddress srcRemoteAddress = new InetSocketAddress("localhost", RCV_PORT);

    private final EventLogger mockLogger = new EventLogger();

    private final FrameHandler nullHandler = new FrameHandler()
    {
        public void onDataFrame(final DataHeaderFlyweight header, final AtomicBuffer buffer,
                                final long length, final InetSocketAddress srcAddress) {}
        public void onStatusMessageFrame(final StatusMessageFlyweight header, final AtomicBuffer buffer,
                                         final long length, final InetSocketAddress srcAddress) {}
        public void onNakFrame(final NakFlyweight header, final AtomicBuffer buffer,
                               final long length, final InetSocketAddress srcAddress) {}
    };

    @Test(timeout = 1000)
    public void shouldHandleBasicSetupAndTeardown() throws Exception
    {
        final NioSelector nioSelector = new NioSelector();
        final UdpTransport rcv = new UdpTransport(nullHandler, rcvLocalAddress, nioSelector, mockLogger);
        final UdpTransport src = new UdpTransport(nullHandler, srcLocalAddress, nioSelector, mockLogger);

        processLoop(nioSelector, 5);
        rcv.close();
        src.close();
        processLoop(nioSelector, 5);
        nioSelector.close();
    }

    @Test(timeout = 1000)
    public void shouldHandleEmptyDataFrameFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersReceived = new AtomicInteger(0);

        final NioSelector nioSelector = new NioSelector();
        final UdpTransport rcv = new UdpTransport(new FrameHandler()
        {
            public void onDataFrame(final DataHeaderFlyweight header, final AtomicBuffer buffer,
                                    final long length, final InetSocketAddress srcAddress)
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
            }
            public void onStatusMessageFrame(final StatusMessageFlyweight header, final AtomicBuffer buffer,
                                             final long length, final InetSocketAddress srcAddress) {}
            public void onNakFrame(final NakFlyweight header, final AtomicBuffer buffer,
                                   final long length, final InetSocketAddress srcAddress) {}
        }, rcvLocalAddress, nioSelector, mockLogger);

        final UdpTransport src = new UdpTransport(nullHandler, srcLocalAddress, nioSelector, mockLogger);

        encodeDataHeader.wrap(atomicBuffer, 0);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                        .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(24);
        encodeDataHeader.sessionId(SESSION_ID)
                        .channelId(CHANNEL_ID)
                        .termId(TERM_ID);
        buffer.position(0).limit(24);

        processLoop(nioSelector, 5);
        src.sendTo(buffer, srcRemoteAddress);
        while (dataHeadersReceived.get() < 1)
        {
            processLoop(nioSelector, 1);
        }
        rcv.close();
        src.close();
        processLoop(nioSelector, 5);
        nioSelector.close();

        assertThat(dataHeadersReceived.get(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldHandleSmFrameFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersReceived = new AtomicInteger(0);
        final AtomicInteger cntlHeadersReceived = new AtomicInteger(0);

        final NioSelector nioSelector = new NioSelector();
        final UdpTransport rcv = new UdpTransport(new FrameHandler()
        {
            public void onDataFrame(final DataHeaderFlyweight header, final AtomicBuffer buffer,
                                    final long length, final InetSocketAddress srcAddress)
            {
                dataHeadersReceived.incrementAndGet();
            }

            public void onStatusMessageFrame(final StatusMessageFlyweight header, final AtomicBuffer buffer,
                                             final long length, final InetSocketAddress srcAddress)
            {
                assertThat(header.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
                assertThat(header.frameLength(), is(StatusMessageFlyweight.HEADER_LENGTH));
                cntlHeadersReceived.incrementAndGet();
            }

            public void onNakFrame(final NakFlyweight header, final AtomicBuffer buffer,
                                   final long length, final InetSocketAddress srcAddress) {}
        }, rcvLocalAddress, nioSelector, mockLogger);

        final UdpTransport src = new UdpTransport(nullHandler, srcLocalAddress, nioSelector, mockLogger);

        statusMessage.wrap(atomicBuffer, 0);
        statusMessage.channelId(CHANNEL_ID)
                     .sessionId(SESSION_ID)
                     .termId(TERM_ID)
                     .receiverWindow(1000)
                     .highestContiguousTermOffset(0)
                     .version(HeaderFlyweight.CURRENT_VERSION)
                     .flags((short) 0)
                     .headerType(HeaderFlyweight.HDR_TYPE_SM)
                     .frameLength(StatusMessageFlyweight.HEADER_LENGTH);
        buffer.position(0).limit(statusMessage.frameLength());

        processLoop(nioSelector, 5);
        src.sendTo(buffer, srcRemoteAddress);
        while (cntlHeadersReceived.get() < 1)
        {
            processLoop(nioSelector, 1);
        }
        rcv.close();
        src.close();
        processLoop(nioSelector, 5);
        nioSelector.close();

        assertThat(dataHeadersReceived.get(), is(0));
        assertThat(cntlHeadersReceived.get(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldHandleMultipleFramesPerDatagramFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersReceived = new AtomicInteger(0);
        final AtomicInteger cntlHeadersReceived = new AtomicInteger(0);

        final NioSelector nioSelector = new NioSelector();
        final UdpTransport rcv = new UdpTransport(new FrameHandler()
        {
            public void onDataFrame(final DataHeaderFlyweight header, final AtomicBuffer buffer,
                                    final long length, final InetSocketAddress srcAddress)
            {
                assertThat(header.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
                assertThat(header.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
                assertThat(header.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(header.frameLength(), is(24));
                assertThat(header.sessionId(), is(SESSION_ID));
                assertThat(header.channelId(), is(CHANNEL_ID));
                assertThat(header.termId(), is(TERM_ID));
                dataHeadersReceived.incrementAndGet();

                // got one Data Frame, but see if there is me in the Datagram...
                if (header.frameLength() <= length)
                {
                    dataHeadersReceived.incrementAndGet();
                }
            }

            public void onNakFrame(final NakFlyweight header, final AtomicBuffer buffer,
                                   final long length, final InetSocketAddress srcAddress)
            {
                cntlHeadersReceived.incrementAndGet();
            }

            public void onStatusMessageFrame(final StatusMessageFlyweight header, final AtomicBuffer buffer,
                                             final long length, final InetSocketAddress srcAddress)
            {
                cntlHeadersReceived.incrementAndGet();
            }
        }, rcvLocalAddress, nioSelector, mockLogger);

        final UdpTransport src = new UdpTransport(nullHandler, srcLocalAddress, nioSelector, mockLogger);

        encodeDataHeader.wrap(atomicBuffer, 0);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                        .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(24);
        encodeDataHeader.sessionId(SESSION_ID)
                        .channelId(CHANNEL_ID)
                        .termId(TERM_ID);
        encodeDataHeader.wrap(atomicBuffer, 24);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                        .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(24);
        encodeDataHeader.sessionId(SESSION_ID)
                        .channelId(CHANNEL_ID)
                        .termId(TERM_ID);
        buffer.position(0).limit(48);

        processLoop(nioSelector, 5);
        src.sendTo(buffer, srcRemoteAddress);
        while (dataHeadersReceived.get() < 1)
        {
            processLoop(nioSelector, 1);
        }
        rcv.close();
        src.close();
        processLoop(nioSelector, 5);
        nioSelector.close();

        assertThat(dataHeadersReceived.get(), is(2));
        assertThat(cntlHeadersReceived.get(), is(0));
    }

    @Test(timeout = 1000)
    public void shouldHandleSmFrameFromReceiverToSender() throws Exception
    {
        final AtomicInteger dataHeadersReceived = new AtomicInteger(0);
        final AtomicInteger cntlHeadersReceived = new AtomicInteger(0);

        final NioSelector nioSelector = new NioSelector();
        final UdpTransport src = new UdpTransport(new FrameHandler()
        {
            public void onDataFrame(final DataHeaderFlyweight header, final AtomicBuffer buffer,
                                    final long length, final InetSocketAddress srcAddress)
            {
                dataHeadersReceived.incrementAndGet();
            }

            public void onStatusMessageFrame(final StatusMessageFlyweight header, final AtomicBuffer buffer,
                                             final long length, final InetSocketAddress srcAddress)
            {
                assertThat(header.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
                assertThat(header.frameLength(), is(StatusMessageFlyweight.HEADER_LENGTH));
                cntlHeadersReceived.incrementAndGet();
            }

            public void onNakFrame(final NakFlyweight header, final AtomicBuffer buffer,
                                   final long length, final InetSocketAddress srcAddress)
            {
                cntlHeadersReceived.incrementAndGet();
            }
        }, srcLocalAddress, nioSelector, mockLogger);

        final UdpTransport rcv = new UdpTransport(nullHandler, rcvLocalAddress, nioSelector, mockLogger);

        statusMessage.wrap(atomicBuffer, 0);
        statusMessage.channelId(CHANNEL_ID)
                .sessionId(SESSION_ID)
                .termId(TERM_ID)
                .receiverWindow(1000)
                .highestContiguousTermOffset(0)
                .version(HeaderFlyweight.CURRENT_VERSION)
                .flags((short) 0)
                .headerType(HeaderFlyweight.HDR_TYPE_SM)
                .frameLength(StatusMessageFlyweight.HEADER_LENGTH);
        buffer.position(0).limit(statusMessage.frameLength());

        processLoop(nioSelector, 5);
        rcv.sendTo(buffer, rcvRemoteAddress);
        while (cntlHeadersReceived.get() < 1)
        {
            processLoop(nioSelector, 1);
        }

        rcv.close();
        src.close();
        processLoop(nioSelector, 5);
        nioSelector.close();

        assertThat(dataHeadersReceived.get(), is(0));
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

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
package uk.co.real_logic.aeron.mediadriver;

import org.junit.Test;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class NioSelectorTest
{
    private static final int RCV_PORT = 40123;
    private static final int SRC_PORT = 40124;
    private static final long SESSION_ID = 0xdeadbeefL;
    private static final long CHANNEL_ID = 0x44332211L;
    private static final long TERM_ID = 0x99887766L;
    private static final String SRC_UDP_URI = "udp://localhost:40124@localhost:40123";
    private static final String RCV_UDP_URI = "udp://localhost:40123";

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(256);
    private final AtomicBuffer atomicBuffer = new AtomicBuffer(buffer);
    private final DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
    private final InetSocketAddress rcvRemoteAddr = new InetSocketAddress("localhost", SRC_PORT);
    private final InetSocketAddress rcvLocalAddr = new InetSocketAddress(RCV_PORT);
    private final InetSocketAddress srcLocalAddr = new InetSocketAddress(SRC_PORT);
    private final InetSocketAddress srcRemoteAddr = new InetSocketAddress("localhost", RCV_PORT);

    private final FrameHandler nullHandler = new FrameHandler() {};

    @Test(timeout = 1000)
    public void shouldHandleBasicSetupAndTeardown() throws Exception
    {
        final NioSelector nioSelector = new NioSelector();
        final UdpTransport rcv = new UdpTransport(nullHandler, rcvLocalAddr, nioSelector);
        final UdpTransport src = new UdpTransport(nullHandler, srcLocalAddr, nioSelector);

        processLoop(nioSelector, 5);
        rcv.close();
        src.close();
        processLoop(nioSelector, 5);
        nioSelector.close();
    }

    @Test(timeout = 1000)
    public void shouldHandleEmptyDataFrameFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersRcved = new AtomicInteger(0);

        final NioSelector nioSelector = new NioSelector();
        final UdpTransport rcv = new UdpTransport(new FrameHandler()
        {
            public void onDataFrame(final DataHeaderFlyweight header, final InetSocketAddress srcAddr)
            {
                assertThat(Byte.valueOf(header.version()), is(Byte.valueOf(HeaderFlyweight.CURRENT_VERSION)));
                assertThat(Integer.valueOf(header.headerType()), is(Integer.valueOf(HeaderFlyweight.HDR_TYPE_DATA)));
                assertThat(Integer.valueOf(header.frameLength()), is(Integer.valueOf(24)));
                assertThat(Long.valueOf(header.sessionId()), is(Long.valueOf(SESSION_ID)));
                assertThat(Long.valueOf(header.channelId()), is(Long.valueOf(CHANNEL_ID)));
                assertThat(Long.valueOf(header.termId()), is(Long.valueOf(TERM_ID)));
                assertThat(Integer.valueOf(header.dataOffset()), is(Integer.valueOf(24)));
                dataHeadersRcved.incrementAndGet();
            }
        }, rcvLocalAddr, nioSelector);

        final UdpTransport src = new UdpTransport(nullHandler, srcLocalAddr, nioSelector);

        encodeDataHeader.reset(atomicBuffer, 0);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(24);
        encodeDataHeader.sessionId(SESSION_ID)
                        .channelId(CHANNEL_ID)
                        .termId(TERM_ID);
        buffer.position(0).limit(24);

        processLoop(nioSelector, 5);
        src.sendTo(buffer, srcRemoteAddr);
        while (dataHeadersRcved.get() < 1)
        {
            processLoop(nioSelector, 1);
        }
        rcv.close();
        src.close();
        processLoop(nioSelector, 5);
        nioSelector.close();

        assertThat(Integer.valueOf(dataHeadersRcved.get()), is(Integer.valueOf(1)));
    }

    @Test(timeout = 1000)
    public void shouldHandleSmFrameFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersRcved = new AtomicInteger(0);
        final AtomicInteger cntlHeadersRcved = new AtomicInteger(0);

        final NioSelector nioSelector = new NioSelector();
        final UdpTransport rcv = new UdpTransport(new FrameHandler()
        {
            public void onDataFrame(final DataHeaderFlyweight header, final InetSocketAddress srcAddr)
            {
                dataHeadersRcved.incrementAndGet();
            }

            public void onControlFrame(final HeaderFlyweight header, final InetSocketAddress srcAddr)
            {
                assertThat(Byte.valueOf(header.version()), is(Byte.valueOf(HeaderFlyweight.CURRENT_VERSION)));
                assertThat(Integer.valueOf(header.headerType()), is(Integer.valueOf(HeaderFlyweight.HDR_TYPE_SM)));
                assertThat(Integer.valueOf(header.frameLength()), is(Integer.valueOf(8)));
                cntlHeadersRcved.incrementAndGet();
            }
        }, rcvLocalAddr, nioSelector);

        final UdpTransport src = new UdpTransport(nullHandler, srcLocalAddr, nioSelector);

        encodeDataHeader.reset(atomicBuffer, 0);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .headerType(HeaderFlyweight.HDR_TYPE_SM)
                        .frameLength(8);
        buffer.position(0).limit(8);

        processLoop(nioSelector, 5);
        src.sendTo(buffer, srcRemoteAddr);
        while (cntlHeadersRcved.get() < 1)
        {
            processLoop(nioSelector, 1);
        }
        rcv.close();
        src.close();
        processLoop(nioSelector, 5);
        nioSelector.close();

        assertThat(Integer.valueOf(dataHeadersRcved.get()), is(Integer.valueOf(0)));
        assertThat(Integer.valueOf(cntlHeadersRcved.get()), is(Integer.valueOf(1)));
    }

    @Test(timeout = 1000)
    public void shouldHandleMultipleFramesPerDatagramFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersRcved = new AtomicInteger(0);
        final AtomicInteger cntlHeadersRcved = new AtomicInteger(0);

        final NioSelector nioSelector = new NioSelector();
        final UdpTransport rcv = new UdpTransport(new FrameHandler()
        {
            public void onDataFrame(final DataHeaderFlyweight header, final InetSocketAddress srcAddr)
            {
                assertThat(Byte.valueOf(header.version()), is(Byte.valueOf(HeaderFlyweight.CURRENT_VERSION)));
                assertThat(Integer.valueOf(header.headerType()), is(Integer.valueOf(HeaderFlyweight.HDR_TYPE_DATA)));
                assertThat(Integer.valueOf(header.frameLength()), is(Integer.valueOf(24)));
                assertThat(Long.valueOf(header.sessionId()), is(Long.valueOf(SESSION_ID)));
                assertThat(Long.valueOf(header.channelId()), is(Long.valueOf(CHANNEL_ID)));
                assertThat(Long.valueOf(header.termId()), is(Long.valueOf(TERM_ID)));
                dataHeadersRcved.incrementAndGet();
            }

            public void onControlFrame(final HeaderFlyweight header, final InetSocketAddress srcAddr)
            {
                cntlHeadersRcved.incrementAndGet();
            }
        }, rcvLocalAddr, nioSelector);

        final UdpTransport src = new UdpTransport(nullHandler, srcLocalAddr, nioSelector);

        encodeDataHeader.reset(atomicBuffer, 0);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(24);
        encodeDataHeader.sessionId(SESSION_ID)
                        .channelId(CHANNEL_ID)
                        .termId(TERM_ID);
        encodeDataHeader.reset(atomicBuffer, 24);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(24);
        encodeDataHeader.sessionId(SESSION_ID)
                        .channelId(CHANNEL_ID)
                        .termId(TERM_ID);
        buffer.position(0).limit(48);

        processLoop(nioSelector, 5);
        src.sendTo(buffer, srcRemoteAddr);
        while (dataHeadersRcved.get() < 1)
        {
            processLoop(nioSelector, 1);
        }
        rcv.close();
        src.close();
        processLoop(nioSelector, 5);
        nioSelector.close();

        assertThat(Integer.valueOf(dataHeadersRcved.get()), is(Integer.valueOf(2)));
        assertThat(Integer.valueOf(cntlHeadersRcved.get()), is(Integer.valueOf(0)));
    }

    @Test(timeout = 1000)
    public void shouldHandleSmFrameFromReceiverToSender() throws Exception
    {
        final AtomicInteger dataHeadersRcved = new AtomicInteger(0);
        final AtomicInteger cntlHeadersRcved = new AtomicInteger(0);

        final NioSelector nioSelector = new NioSelector();
        final UdpTransport src = new UdpTransport(new FrameHandler()
        {
            public void onDataFrame(final DataHeaderFlyweight header, final InetSocketAddress srcAddr)
            {
                dataHeadersRcved.incrementAndGet();
            }

            public void onControlFrame(final HeaderFlyweight header, final InetSocketAddress srcAddr)
            {
                assertThat(Byte.valueOf(header.version()), is(Byte.valueOf(HeaderFlyweight.CURRENT_VERSION)));
                assertThat(Integer.valueOf(header.headerType()), is(Integer.valueOf(HeaderFlyweight.HDR_TYPE_SM)));
                assertThat(Integer.valueOf(header.frameLength()), is(Integer.valueOf(8)));
                cntlHeadersRcved.incrementAndGet();
            }
        }, srcLocalAddr, nioSelector);

        final UdpTransport rcv = new UdpTransport(nullHandler, rcvLocalAddr, nioSelector);

        encodeDataHeader.reset(atomicBuffer, 0);
        encodeDataHeader.version(HeaderFlyweight.CURRENT_VERSION)
                        .headerType(HeaderFlyweight.HDR_TYPE_SM)
                        .frameLength(8);
        buffer.position(0).limit(8);

        processLoop(nioSelector, 5);
        rcv.sendTo(buffer, rcvRemoteAddr);
        while (cntlHeadersRcved.get() < 1)
        {
            processLoop(nioSelector, 1);
        }

        rcv.close();
        src.close();
        processLoop(nioSelector, 5);
        nioSelector.close();

        assertThat(Integer.valueOf(dataHeadersRcved.get()), is(Integer.valueOf(0)));
        assertThat(Integer.valueOf(cntlHeadersRcved.get()), is(Integer.valueOf(1)));
    }

    private void processLoop(final NioSelector nioSelector, final int iterations) throws Exception
    {
        for (int i = 0; i < iterations; i++)
        {
            nioSelector.processKeys();
        }
    }
}

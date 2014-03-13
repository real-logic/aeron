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
import uk.co.real_logic.aeron.util.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.HeaderFlyweight;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EventLoopTest
{
    private static final int RCV_PORT = 40123;
    private static final int SRC_PORT = 40124;
    private static final long SESSION_ID = 0xdeadbeefL;
    private static final long CHANNEL_ID = 0x44332211L;
    private static final long TERM_ID = 0x99887766L;
    private static final String SRC_UDP_URI = "udp://localhost:40124@localhost:40123";
    private static final String RCV_UDP_URI = "udp://localhost:40123@localhost:40124";

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(256);
    private final DirectBuffer directBuffer = new DirectBuffer(buffer);
    private final DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
    private final InetSocketAddress srcRemoteAddr = new InetSocketAddress("localhost", RCV_PORT);
    private final InetSocketAddress rcvRemoteAddr = new InetSocketAddress("localhost", SRC_PORT);
    private final InetSocketAddress rcvLocalAddr = new InetSocketAddress(RCV_PORT);
    private final InetSocketAddress srcLocalAddr = new InetSocketAddress(SRC_PORT);

    @Test(timeout = 1000)
    public void shouldHandleBasicSetupAndTeardown() throws Exception
    {
        final EventLoop eventLoop = new EventLoop();
        final RcvFrameHandler rcv = new RcvFrameHandler(rcvLocalAddr, eventLoop);
        final SrcFrameHandler src = new SrcFrameHandler(UdpDestination.parse(SRC_UDP_URI), eventLoop);

        processLoop(eventLoop, 5);
        rcv.close();
        src.close();
        processLoop(eventLoop, 5);
        eventLoop.close();
    }

    @Test(timeout = 1000)
    public void shouldHandleEmptyDataFrameFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersRcved = new AtomicInteger(0);

        final EventLoop eventLoop = new EventLoop();
        final UdpTransport rcv = new UdpTransport(new FrameHandler()
        {
            public void onDataFrame(DataHeaderFlyweight header, InetSocketAddress srcAddr)
            {
                assertThat(header.version(), is((byte)HeaderFlyweight.CURRENT_VERSION));
                assertThat(header.headerType(), is((short)HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(header.frameLength(), is(20));
                assertThat(header.sessionId(), is(SESSION_ID));
                assertThat(header.channelId(), is(CHANNEL_ID));
                assertThat(header.termId(), is(TERM_ID));
                assertThat(header.dataOffset(), is(20));
                dataHeadersRcved.incrementAndGet();
            }

            public void onControlFrame(HeaderFlyweight header, InetSocketAddress srcAddr)
            {
            }
        }, rcvLocalAddr, eventLoop);

        final SrcFrameHandler src = new SrcFrameHandler(UdpDestination.parse(SRC_UDP_URI), eventLoop);

        encodeDataHeader.reset(directBuffer, 0)
                        .version((byte)HeaderFlyweight.CURRENT_VERSION)
                        .headerType((short)HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(20)
                        .sessionId(SESSION_ID);
        encodeDataHeader.channelId(CHANNEL_ID)
                        .termId(TERM_ID);
        buffer.position(0).limit(20);

        processLoop(eventLoop, 5);
        src.send(buffer);
        while (dataHeadersRcved.get() < 1)
        {
            processLoop(eventLoop, 1);
        }
        rcv.close();
        src.close();
        processLoop(eventLoop, 5);
        eventLoop.close();

        assertThat(dataHeadersRcved.get(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldHandleConnFrameFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersRcved = new AtomicInteger(0);
        final AtomicInteger cntlHeadersRcved = new AtomicInteger(0);

        final EventLoop eventLoop = new EventLoop();
        final UdpTransport rcv = new UdpTransport(new FrameHandler()
        {
            public void onDataFrame(DataHeaderFlyweight header, InetSocketAddress srcAddr)
            {
                dataHeadersRcved.incrementAndGet();
            }

            public void onControlFrame(HeaderFlyweight header, InetSocketAddress srcAddr)
            {
                assertThat(header.version(), is((byte)HeaderFlyweight.CURRENT_VERSION));
                assertThat(header.headerType(), is((short)HeaderFlyweight.HDR_TYPE_CONN));
                assertThat(header.frameLength(), is(8));
                assertThat(header.sessionId(), is(SESSION_ID));
                cntlHeadersRcved.incrementAndGet();
            }
        }, rcvLocalAddr, eventLoop);

        final SrcFrameHandler src = new SrcFrameHandler(UdpDestination.parse(SRC_UDP_URI), eventLoop);

        encodeDataHeader.reset(directBuffer, 0)
                        .version((byte)HeaderFlyweight.CURRENT_VERSION)
                        .headerType((short)HeaderFlyweight.HDR_TYPE_CONN)
                        .frameLength(8)
                        .sessionId(SESSION_ID);
        buffer.position(0).limit(8);

        processLoop(eventLoop, 5);
        src.send(buffer);
        while (cntlHeadersRcved.get() < 1)
        {
            processLoop(eventLoop, 1);
        }
        rcv.close();
        src.close();
        processLoop(eventLoop, 5);
        eventLoop.close();

        assertThat(dataHeadersRcved.get(), is(0));
        assertThat(cntlHeadersRcved.get(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldHandleMultipleFramesPerDatagramFromSourceToReceiver() throws Exception
    {
        final AtomicInteger dataHeadersRcved = new AtomicInteger(0);
        final AtomicInteger cntlHeadersRcved = new AtomicInteger(0);

        final EventLoop eventLoop = new EventLoop();
        final UdpTransport rcv = new UdpTransport(new FrameHandler()
        {
            public void onDataFrame(DataHeaderFlyweight header, InetSocketAddress srcAddr)
            {
                assertThat(header.version(), is((byte)HeaderFlyweight.CURRENT_VERSION));
                assertThat(header.headerType(), is((short)HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(header.frameLength(), is(20));
                assertThat(header.sessionId(), is(SESSION_ID));
                assertThat(header.channelId(), is(CHANNEL_ID));
                assertThat(header.termId(), is(TERM_ID));
                dataHeadersRcved.incrementAndGet();
            }

            public void onControlFrame(HeaderFlyweight header, InetSocketAddress srcAddr)
            {
                cntlHeadersRcved.incrementAndGet();
            }
        }, rcvLocalAddr, eventLoop);

        final SrcFrameHandler src = new SrcFrameHandler(UdpDestination.parse(SRC_UDP_URI), eventLoop);

        encodeDataHeader.reset(directBuffer, 0)
                        .version((byte) HeaderFlyweight.CURRENT_VERSION)
                        .headerType((short) HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(20)
                        .sessionId(SESSION_ID);
        encodeDataHeader.channelId(CHANNEL_ID)
                        .termId(TERM_ID);
        encodeDataHeader.reset(directBuffer, 20)
                        .version((byte)HeaderFlyweight.CURRENT_VERSION)
                        .headerType((short)HeaderFlyweight.HDR_TYPE_DATA)
                        .frameLength(20)
                        .sessionId(SESSION_ID);
        encodeDataHeader.channelId(CHANNEL_ID)
                        .termId(TERM_ID);
        buffer.position(0).limit(40);

        processLoop(eventLoop, 5);
        src.send(buffer);
        while (dataHeadersRcved.get() < 1)
        {
            processLoop(eventLoop, 1);
        }
        rcv.close();
        src.close();
        processLoop(eventLoop, 5);
        eventLoop.close();

        assertThat(dataHeadersRcved.get(), is(2));
        assertThat(cntlHeadersRcved.get(), is(0));
    }

    @Test(timeout = 1000)
    public void shouldHandleConnFrameFromReceiverToSender() throws Exception
    {
        final AtomicInteger dataHeadersRcved = new AtomicInteger(0);
        final AtomicInteger cntlHeadersRcved = new AtomicInteger(0);

        final EventLoop eventLoop = new EventLoop();
        final UdpTransport src = new UdpTransport(new FrameHandler()
        {
            public void onDataFrame(DataHeaderFlyweight header, InetSocketAddress srcAddr)
            {
                dataHeadersRcved.incrementAndGet();
            }

            public void onControlFrame(HeaderFlyweight header, InetSocketAddress srcAddr)
            {
                assertThat(header.version(), is((byte)HeaderFlyweight.CURRENT_VERSION));
                assertThat(header.headerType(), is((short)HeaderFlyweight.HDR_TYPE_CONN));
                assertThat(header.frameLength(), is(8));
                assertThat(header.sessionId(), is(SESSION_ID));
                cntlHeadersRcved.incrementAndGet();
            }
        }, srcLocalAddr, eventLoop);

        final RcvFrameHandler rcv = new RcvFrameHandler(rcvLocalAddr, eventLoop);

        encodeDataHeader.reset(directBuffer, 0)
                        .version((byte) HeaderFlyweight.CURRENT_VERSION)
                        .headerType((short) HeaderFlyweight.HDR_TYPE_CONN)
                        .frameLength(8)
                        .sessionId(SESSION_ID);
        buffer.position(0).limit(8);

        processLoop(eventLoop, 5);
        rcv.sendTo(buffer, rcvRemoteAddr);
        while (cntlHeadersRcved.get() < 1)
        {
            processLoop(eventLoop, 1);
        }
        rcv.close();
        src.close();
        processLoop(eventLoop, 5);
        eventLoop.close();

        assertThat(dataHeadersRcved.get(), is(0));
        assertThat(cntlHeadersRcved.get(), is(1));
    }

    private void processLoop(final EventLoop eventLoop, final int iterations)
    {
        for (int i = 0; i < iterations; i++)
        {
            eventLoop.process();
        }
    }
}

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
package uk.co.real_logic.aeron.iodriver;

import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.aeron.util.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.HeaderFlyweight;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EventLoopTest
{
    private static final int PORT = 40123;

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(256);
    private final DirectBuffer dBuff = new DirectBuffer(buffer);
    private final HeaderFlyweight encodeHeader = new HeaderFlyweight();
    private final HeaderFlyweight decodeHeader = new HeaderFlyweight();
    private final DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
    private final DataHeaderFlyweight decodeDataHeader = new DataHeaderFlyweight();
    private final InetSocketAddress srcRemoteAddr = new InetSocketAddress("localhost", PORT);
    private final InetSocketAddress rcvLocalAddr = new InetSocketAddress(PORT);
    private final InetSocketAddress srcLocalAddr = new InetSocketAddress(0);

    @Test(timeout = 100)
    public void shouldHandleBasicSetupAndTeardown() throws Exception
    {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final EventLoop evLoop = new EventLoop();
        final RcvFrameHandler rcv = new RcvFrameHandler(rcvLocalAddr, evLoop);
        final SrcFrameHandler src = new SrcFrameHandler(srcLocalAddr, srcRemoteAddr, evLoop);
        final Future<?> loopFuture = executor.submit(evLoop);

        rcv.close();
        src.close();
        evLoop.close();
        loopFuture.get(100, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 100)
    public void shouldHandleEmptyDataFrameFromSourceToReceiver() throws Exception
    {
        final EventLoop evLoop = new EventLoop();
        final UDPChannel rcv = new UDPChannel(new FrameHandler() {
            @Override
            public void handleDataFrame(DataHeaderFlyweight header, InetSocketAddress srcAddr)
            {
                assertThat(header.version(), is((byte) HeaderFlyweight.CURRENT_VERSION));
                assertThat(header.headerType(), is((short) HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(header.frameLength(), is(20));
                assertThat(header.sessionId(), is(0xdeadbeefL));
                assertThat(header.channelId(), is(0x44332211L));
                assertThat(header.termId(), is(0x99887766L));
                assertThat(header.dataOffset(), is(20));
            }

            @Override
            public void handleControlFrame(HeaderFlyweight header, InetSocketAddress srcAddr)
            {

            }
        }, rcvLocalAddr, evLoop);

        final SrcFrameHandler src = new SrcFrameHandler(srcLocalAddr, srcRemoteAddr, evLoop);

        encodeDataHeader.reset(dBuff, 0);

        encodeDataHeader.version((byte)HeaderFlyweight.CURRENT_VERSION);
        encodeDataHeader.headerType((short)HeaderFlyweight.HDR_TYPE_DATA);
        encodeDataHeader.frameLength(20);
        encodeDataHeader.sessionId(0xdeadbeefL);
        encodeDataHeader.channelId(0x44332211L);
        encodeDataHeader.termId(0x99887766L);
        buffer.position(0);
        buffer.limit(20);

        evLoop.process();
        src.send(buffer);
        IntStream.range(0,4).forEach(nbr -> evLoop.process());
        rcv.close();
        src.close();
        evLoop.close();
        //assertThat(headersRcved, is(1));

        // TODO: make this assert on receiving at least one message
        // TODO: abstract out session ID, channel ID, and term ID
        // TODO: reuse this and use lambda for assert section in
    }

    @Ignore
    @Test
    public void shouldHandleConnFrameFromSourceToReceiver()
    {

    }

    @Ignore
    @Test
    public void shouldHandleConnFrameFromReceiverToSender()
    {

    }

    @Ignore
    @Test
    public void shouldHandleMultipleFramesPerMessageFromSourceToReceiver()
    {

    }
}

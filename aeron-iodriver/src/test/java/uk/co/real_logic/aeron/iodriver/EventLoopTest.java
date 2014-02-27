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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

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

    @Test
    public void shouldHandleBasicSetupAndTeardown()
    {
        Executor executor = Executors.newSingleThreadExecutor();

        try (final EventLoop evLoop = new EventLoop())
        {
            RcvFrameHandler rcv = new RcvFrameHandler(rcvLocalAddr, evLoop);
            SrcFrameHandler src = new SrcFrameHandler(srcLocalAddr, srcRemoteAddr, evLoop);

            executor.execute(evLoop);

            rcv.close();
            src.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    @Ignore
    @Test
    public void shouldBeAbleToSendToReceiverFromSource()
    {
        encodeDataHeader.reset(dBuff, 0);

        encodeDataHeader.version((byte)1);
        encodeDataHeader.headerType((short)HeaderFlyweight.HDR_TYPE_DATA);
        encodeDataHeader.frameLength(8);
        encodeDataHeader.sessionId(0xdeadbeefL);
        encodeDataHeader.channelId(0x44332211L);
        encodeDataHeader.termId(0x99887766L);

        //src.send(buffer);

        // TODO: need to add asserts on incoming header to make sure they work
    }

    @Ignore
    @Test
    public void shouldBeAbleToSendToSourceFromReceiver()
    {

    }
}

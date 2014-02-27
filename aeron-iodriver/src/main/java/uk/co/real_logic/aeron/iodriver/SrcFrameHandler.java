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

import uk.co.real_logic.aeron.util.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.HeaderFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Frame processing for sources
 */
public class SrcFrameHandler implements FrameHandler
{
    private final UDPChannel channel;
    private final InetSocketAddress remoteAddr;

    public SrcFrameHandler(final InetSocketAddress local, final InetSocketAddress remoteAddr, final EventLoop loop) throws Exception
    {
        this.channel = new UDPChannel(this, local, loop);
        this.remoteAddr = remoteAddr;
    }

    public int send(final ByteBuffer buffer) throws Exception
    {
        return channel.sendto(buffer, remoteAddr);
    }

    public void close()
    {
        channel.close();
    }

    @Override
    public void handleDataFrame(final DataHeaderFlyweight header, final InetSocketAddress srcAddr)
    {
        // no frames should come in this way, so just drop it silently.
    }

    @Override
    public void handleControlFrame(final HeaderFlyweight header, final InetSocketAddress srcAddr)
    {
        // TODO: these pretty much just go right onto the control buffer for the API
    }
}

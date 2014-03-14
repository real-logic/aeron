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

import uk.co.real_logic.aeron.util.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.HeaderFlyweight;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Frame processing for receivers
 */
public class RcvFrameHandler implements FrameHandler, AutoCloseable
{
    private final UdpTransport transport;
    private final UdpDestination destination;

    public RcvFrameHandler(final UdpDestination destination, final EventLoop loop) throws Exception
    {
        final InetSocketAddress endpoint = destination.remote();
        final InetAddress mcastInterface = destination.local().getAddress();
        final int localPort = destination.local().getPort();

        this.transport = new UdpTransport(this, endpoint, mcastInterface, localPort, loop);
        this.destination = destination;
    }

    public int sendTo(final ByteBuffer buffer, final long sessionId)
    {
        // TODO: look up sessionId to find saved InetSocketAddress and transport.sendTo
        return 0;
    }

    public int sendTo(final ByteBuffer buffer, final InetSocketAddress addr) throws Exception
    {
        return transport.sendTo(buffer, addr);
    }

    public void close()
    {
        transport.close();
    }

    public UdpDestination destination()
    {
        return destination;
    }

    public void onDataFrame(final DataHeaderFlyweight header, final InetSocketAddress srcAddr)
    {
        // TODO: demux onto Terms for API to pick up
    }

    public void onControlFrame(final HeaderFlyweight header, final InetSocketAddress srcAddr)
    {
        // TODO: these pretty much just go right onto the control buffer for the API
    }
}

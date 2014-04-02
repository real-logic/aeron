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

import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Frame processing for sources
 */
public class SrcFrameHandler implements FrameHandler, AutoCloseable
{
    private final UdpTransport transport;
    private final UdpDestination destination;
    private final RingBuffer adminThreadCommandBuffer;

    public SrcFrameHandler(final UdpDestination destination,
                           final ReceiverThread receiverThread,
                           final RingBuffer adminThreadCommandBuffer) throws Exception
    {
        this.transport = new UdpTransport(this, destination.local(), receiverThread);
        this.destination = destination;
        this.adminThreadCommandBuffer = adminThreadCommandBuffer;
    }

    public int send(final ByteBuffer buffer) throws Exception
    {
        return transport.sendTo(buffer, destination.remote());
    }

    public int sendTo(final ByteBuffer buffer, final InetSocketAddress addr) throws Exception
    {
        return transport.sendTo(buffer, addr);
    }

    @Override
    public void close()
    {
        transport.close();
    }

    public boolean isOpen()
    {
        return transport.isOpen();
    }

    public UdpDestination destination()
    {
        return destination;
    }

    @Override
    public void onDataFrame(final DataHeaderFlyweight header, final InetSocketAddress srcAddr)
    {
        // we don't care, so just drop it silently.
    }

    @Override
    public void onControlFrame(final HeaderFlyweight header, final InetSocketAddress srcAddr)
    {
        // dispatch frames to Admin or Sender Threads to handle
        if (header.headerType() == HeaderFlyweight.HDR_TYPE_NAK)
        {
            MediaDriverAdminThread.addNakEvent(adminThreadCommandBuffer, header);
        }
        else if (header.headerType() == HeaderFlyweight.HDR_TYPE_SM)
        {
            // TODO: inform the sender thread so that it can process the SM
        }
    }
}

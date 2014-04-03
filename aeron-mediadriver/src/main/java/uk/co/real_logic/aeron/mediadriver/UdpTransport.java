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

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.NakFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;

import static uk.co.real_logic.aeron.util.protocol.HeaderFlyweight.*;

/**
 * Transport abstraction for UDP sources and receivers.
 *
 * We don't conflate the processing logic, or we at least try not to, into this object.
 *
 * Holds DatagramChannel, read Buffer, etc.
 */
public final class UdpTransport implements ReadHandler, AutoCloseable
{
    private final ByteBuffer readByteBuffer = ByteBuffer.allocateDirect(MediaDriver.READ_BYTE_BUFFER_SZ);
    private final AtomicBuffer readBuffer;
    private final DatagramChannel channel = DatagramChannel.open();
    private final HeaderFlyweight header = new HeaderFlyweight();
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final NakFlyweight nak = new NakFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();
    private final FrameHandler frameHandler;
    private final NioSelector nioSelector;
    private final SelectionKey registeredKey;

    public UdpTransport(final FrameHandler frameHandler,
                        final InetSocketAddress local,
                        final NioSelector nioSelector) throws Exception
    {
        this.readBuffer = new AtomicBuffer(this.readByteBuffer);
        this.frameHandler = frameHandler;
        this.nioSelector = nioSelector;

        channel.bind(local);
        channel.configureBlocking(false);
        this.registeredKey = nioSelector.registerForRead(channel, this);
    }

    public UdpTransport(final RcvFrameHandler frameHandler,
                        final InetSocketAddress local,
                        final InetAddress mcastInterfaceAddr,
                        final int localPort,
                        final NioSelector nioSelector) throws Exception
    {
        this.readBuffer = new AtomicBuffer(this.readByteBuffer);
        this.frameHandler = frameHandler;
        this.nioSelector = nioSelector;

        if (local.getAddress().isMulticastAddress())
        {
            final NetworkInterface mcastInterface = NetworkInterface.getByInetAddress(mcastInterfaceAddr);

            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            channel.bind(new InetSocketAddress(mcastInterfaceAddr, localPort));

            channel.join(local.getAddress(), mcastInterface);
        }
        else
        {
            channel.bind(local);
        }

        channel.configureBlocking(false);
        this.registeredKey = nioSelector.registerForRead(channel, this);
    }

    public int sendTo(final ByteBuffer buffer, final InetSocketAddress remote) throws Exception
    {
        return channel.send(buffer, remote);
    }

    public boolean isOpen()
    {
        return channel.isOpen();
    }

    public void close()
    {
        try
        {
            nioSelector.cancelRead(registeredKey);
            channel.close();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    public void onRead() throws Exception
    {
        readByteBuffer.clear();
        final InetSocketAddress srcAddr = (InetSocketAddress)channel.receive(readByteBuffer);
        final int len = readByteBuffer.position();
        int offset = 0;

        if (srcAddr == null)
        {
            return;
        }

        // parse through buffer for each Frame.
        while (offset < len)
        {
            header.reset(readBuffer, offset);

            // drop a version we don't know
            if (header.version() != HeaderFlyweight.CURRENT_VERSION)
            {
                continue;
            }

            switch (header.headerType())
            {
                case HDR_TYPE_DATA:
                    dataHeader.reset(readBuffer, offset);
                    frameHandler.onDataFrame(dataHeader, srcAddr);
                    break;
                case HDR_TYPE_NAK:
                    nak.reset(readBuffer, offset);
                    frameHandler.onNakFrame(nak, srcAddr);
                    break;
                case HDR_TYPE_SM:
                    statusMessage.reset(readBuffer, offset);
                    frameHandler.onStatusMessageFrame(statusMessage, srcAddr);
                    break;
                default:
                    frameHandler.onControlFrame(header, srcAddr);
                    break;
            }

            offset += header.frameLength();
        }
    }
}

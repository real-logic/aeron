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
import uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.util.protocol.*;

import java.net.*;
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
        registeredKey = nioSelector.registerForRead(channel, this);
    }

    public UdpTransport(final DataFrameHandler frameHandler,
                        final UdpDestination destination,
                        final NioSelector nioSelector) throws Exception
    {
        this.readBuffer = new AtomicBuffer(this.readByteBuffer);
        this.frameHandler = frameHandler;
        this.nioSelector = nioSelector;

        if (destination.isMulticast())
        {
            final InetAddress endPoint = destination.remoteData().getAddress();
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
            channel.bind(destination.localData());
            channel.join(endPoint, destination.localDataInterface());
        }
        else
        {
            channel.bind(destination.remoteData());
        }

        channel.configureBlocking(false);
        registeredKey = nioSelector.registerForRead(channel, this);
    }

    public int sendTo(final ByteBuffer buffer, final InetSocketAddress remote) throws Exception
    {
        return channel.send(buffer, remote);
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

    public DatagramChannel channel()
    {
        return channel;
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
            header.wrap(readBuffer, offset);

//            System.out.println("onRead " + header.frameLength() + " offset " + offset + " type " + header.headerType());
            // drop a version we don't know
            if (header.version() != HeaderFlyweight.CURRENT_VERSION)
            {
                continue;
            }

            // malformed, so log and break out of entire packet
            if (header.frameLength() <= FrameDescriptor.BASE_HEADER_LENGTH)
            {
                System.err.println("received malformed frameLength (" + header.frameLength() + "), dropping");
                break;
            }


            switch (header.headerType())
            {
                case HDR_TYPE_DATA:
                    dataHeader.wrap(readBuffer, offset);
                    frameHandler.onDataFrame(dataHeader, srcAddr);
                    break;

                case HDR_TYPE_NAK:
                    nak.wrap(readBuffer, offset);
                    frameHandler.onNakFrame(nak, srcAddr);
                    break;

                case HDR_TYPE_SM:
                    statusMessage.wrap(readBuffer, offset);
                    frameHandler.onStatusMessageFrame(statusMessage, srcAddr);
                    break;

                default:
                    System.err.println("received unknown type (" + header.headerType() + "), dropping");
                    break;
            }

            offset += header.frameLength();
        }
    }
}

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

import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.common.event.EventCode;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.*;

import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;

import static uk.co.real_logic.aeron.common.protocol.HeaderFlyweight.*;

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
    private final NakFlyweight nakHeader = new NakFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();
    private final FrameHandler frameHandler;
    private final NioSelector nioSelector;
    private final SelectionKey registeredKey;
    private final boolean multicast;

    /*
     * Generic constructor. Used mainly for selector testing.
     */
    public UdpTransport(final FrameHandler frameHandler,
                        final InetSocketAddress local,
                        final NioSelector nioSelector) throws Exception
    {
        this.readBuffer = new AtomicBuffer(this.readByteBuffer);
        wrapHeadersOnReadBuffer();

        this.frameHandler = frameHandler;
        this.nioSelector = nioSelector;

        channel.bind(local);
        channel.configureBlocking(false);
        registeredKey = nioSelector.registerForRead(channel, this);

        multicast = false;
    }

    public UdpTransport(final ControlFrameHandler frameHandler,
                        final UdpDestination destination,
                        final NioSelector nioSelector) throws Exception
    {
        this.readBuffer = new AtomicBuffer(this.readByteBuffer);
        wrapHeadersOnReadBuffer();

        this.frameHandler = frameHandler;
        this.nioSelector = nioSelector;

        if (destination.isMulticast())
        {
            final InetAddress endPointAddress = destination.remoteControl().getAddress();
            final int dstPort = destination.remoteControl().getPort();
            final NetworkInterface localInterface = destination.localInterface();
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            channel.bind(new InetSocketAddress(dstPort));
            channel.join(endPointAddress, localInterface);
            channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, localInterface);
            multicast = true;
        }
        else
        {
            channel.bind(destination.localControl());
            multicast = false;
        }

        channel.configureBlocking(false);
        registeredKey = nioSelector.registerForRead(channel, this);
    }

    public UdpTransport(final DataFrameHandler frameHandler,
                        final UdpDestination destination,
                        final NioSelector nioSelector) throws Exception
    {
        this.readBuffer = new AtomicBuffer(this.readByteBuffer);
        wrapHeadersOnReadBuffer();

        this.frameHandler = frameHandler;
        this.nioSelector = nioSelector;

        if (destination.isMulticast())
        {
            final InetAddress endPointAddress = destination.remoteData().getAddress();
            final int dstPort = destination.remoteData().getPort();
            final NetworkInterface localInterface = destination.localInterface();
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            channel.bind(new InetSocketAddress(dstPort));
            channel.join(endPointAddress, localInterface);
            channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, localInterface);
            multicast = true;
        }
        else
        {
            channel.bind(destination.remoteData());
            multicast = false;
        }

        channel.configureBlocking(false);
        registeredKey = nioSelector.registerForRead(channel, this);
    }

    public int sendTo(final ByteBuffer buffer, final InetSocketAddress remoteAddress) throws Exception
    {
        EventLogger.log(EventCode.FRAME_OUT, buffer, buffer.remaining(), remoteAddress);

        return channel.send(buffer, remoteAddress);
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
            EventLogger.logException(ex);
        }
    }

    public DatagramChannel channel()
    {
        return channel;
    }

    public boolean isMulticast()
    {
        return multicast;
    }

    public int onRead() throws Exception
    {
        readByteBuffer.clear();
        final InetSocketAddress srcAddress = (InetSocketAddress)channel.receive(readByteBuffer);

        if (srcAddress == null)
        {
            return 0;
        }

        // each datagram will start with a frame and have 1 or more frames per datagram

        final int length = readByteBuffer.position();
        EventLogger.log(EventCode.FRAME_IN, readByteBuffer, length, srcAddress);

        // drop a version we don't know
        if (header.version() != HeaderFlyweight.CURRENT_VERSION)
        {
            return 0;
        }

        // malformed, so log and break out of entire packet
        if (header.frameLength() <= FrameDescriptor.BASE_HEADER_LENGTH)
        {
            EventLogger.log(EventCode.MALFORMED_FRAME_LENGTH, readBuffer, 0, HeaderFlyweight.HEADER_LENGTH);
            return 0;
        }

        switch (header.headerType())
        {
            case HDR_TYPE_DATA:
                frameHandler.onDataFrame(dataHeader, readBuffer, length, srcAddress);
                break;

            case HDR_TYPE_NAK:
                frameHandler.onNakFrame(nakHeader, readBuffer, length, srcAddress);
                break;

            case HDR_TYPE_SM:
                frameHandler.onStatusMessageFrame(statusMessage, readBuffer, length, srcAddress);
                break;

            default:
                EventLogger.log(EventCode.UNKNOWN_HEADER_TYPE, readBuffer, 0, HeaderFlyweight.HEADER_LENGTH);
                break;
        }

        return 1;
    }

    private void wrapHeadersOnReadBuffer()
    {
        header.wrap(readBuffer, 0);
        dataHeader.wrap(readBuffer, 0);
        nakHeader.wrap(readBuffer, 0);
        statusMessage.wrap(readBuffer, 0);
    }
}

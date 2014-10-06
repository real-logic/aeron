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
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;

public abstract class UdpChannelTransport implements AutoCloseable
{
    protected final DatagramChannel datagramChannel;
    protected final UdpChannel udpChannel;
    protected final ByteBuffer readByteBuffer = ByteBuffer.allocateDirect(Configuration.READ_BYTE_BUFFER_SZ);
    protected final AtomicBuffer readBuffer = new AtomicBuffer(readByteBuffer);
    protected final HeaderFlyweight header = new HeaderFlyweight();
    protected final EventLogger logger;
    protected final boolean multicast;
    protected final LossGenerator lossGenerator;
    protected SelectionKey registeredKey;

    public UdpChannelTransport(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointSocketAddress,
        final InetSocketAddress bindAddress,
        final LossGenerator lossGenerator,
        final EventLogger logger)
    {
        this.udpChannel = udpChannel;
        this.lossGenerator = lossGenerator;
        this.logger = logger;

        header.wrap(readBuffer, 0);

        try
        {
            datagramChannel = DatagramChannel.open();
            if (udpChannel.isMulticast())
            {
                final NetworkInterface localInterface = udpChannel.localInterface();

                datagramChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                datagramChannel.bind(new InetSocketAddress(endPointSocketAddress.getPort()));
                datagramChannel.join(endPointSocketAddress.getAddress(), localInterface);
                datagramChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, localInterface);
                multicast = true;
            }
            else
            {
                datagramChannel.bind(bindAddress);
                multicast = false;
            }

            if (0 != Configuration.SOCKET_RCVBUF_SZ)
            {
                datagramChannel.setOption(StandardSocketOptions.SO_RCVBUF, Configuration.SOCKET_RCVBUF_SZ);
            }

            if (0 != Configuration.SOCKET_SNDBUF_SZ)
            {
                datagramChannel.setOption(StandardSocketOptions.SO_SNDBUF, Configuration.SOCKET_SNDBUF_SZ);
            }

            datagramChannel.configureBlocking(false);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(
                String.format("channel \"%s\" : %s", udpChannel.originalUriString(), ex.toString()), ex);
        }
    }

    /**
     * Return underlying {@link uk.co.real_logic.aeron.driver.UdpChannel}
     *
     * @return underlying channel
     */
    public UdpChannel udpChannel()
    {
        return udpChannel;
    }

    /**
     * Send contents of {@link java.nio.ByteBuffer} to remote address
     *
     * @param buffer        to send
     * @param remoteAddress to send to
     * @return number of bytes sent
     */
    public int sendTo(final ByteBuffer buffer, final InetSocketAddress remoteAddress)
    {
        logger.log(EventCode.FRAME_OUT, buffer, buffer.position(), buffer.remaining(), remoteAddress);

        try
        {
            return datagramChannel.send(buffer, remoteAddress);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Close transport, canceling any pending read operations and closing channel
     */
    public void close()
    {
        try
        {
            if (null != registeredKey)
            {
                registeredKey.cancel();
            }

            datagramChannel.close();
        }
        catch (final Exception ex)
        {
            logger.logException(ex);
        }
    }

    /**
     * Is transport representing a multicast media or unicast
     *
     * @return if transport is multicast media
     */
    public boolean isMulticast()
    {
        return multicast;
    }

    /**
     * Register transport with {@link uk.co.real_logic.aeron.driver.NioSelector} for reading from the channel
     *
     * @param nioSelector to register read with
     */
    public abstract void registerForRead(final NioSelector nioSelector);

    protected boolean isFrameValid(final int length)
    {
        boolean isFrameValid = true;

        if (header.version() != HeaderFlyweight.CURRENT_VERSION)
        {
            logger.log(EventCode.INVALID_VERSION, readBuffer, 0, header.frameLength());
            isFrameValid = false;
        }
        else if (length <= FrameDescriptor.BASE_HEADER_LENGTH)
        {
            logger.log(EventCode.MALFORMED_FRAME_LENGTH, readBuffer, 0, length);
            isFrameValid = false;
        }

        return isFrameValid;
    }

    protected InetSocketAddress receiveFrame()
    {
        readByteBuffer.clear();

        try
        {
            return (InetSocketAddress)datagramChannel.receive(readByteBuffer);
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}

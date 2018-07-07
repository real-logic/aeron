/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.driver.media;

import io.aeron.driver.Configuration;
import io.aeron.exceptions.AeronException;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;

import static io.aeron.logbuffer.FrameDescriptor.frameVersion;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;

public abstract class UdpChannelTransport implements AutoCloseable
{
    protected final UdpChannel udpChannel;
    protected final AtomicCounter invalidPackets;
    protected final DistinctErrorLog errorLog;
    protected UdpTransportPoller transportPoller;

    protected SelectionKey selectionKey;
    protected InetSocketAddress bindAddress;
    protected InetSocketAddress endPointAddress;
    protected InetSocketAddress connectAddress;
    protected DatagramChannel sendDatagramChannel;
    protected DatagramChannel receiveDatagramChannel;
    protected int multicastTtl = 0;
    protected boolean isClosed = false;

    public UdpChannelTransport(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final DistinctErrorLog errorLog,
        final AtomicCounter invalidPackets)
    {
        this.udpChannel = udpChannel;
        this.errorLog = errorLog;
        this.endPointAddress = endPointAddress;
        this.bindAddress = bindAddress;
        this.connectAddress = connectAddress;
        this.invalidPackets = invalidPackets;
    }

    /**
     * Throw a {@link AeronException} with a message for a send error.
     *
     * @param bytesToSend expected to be sent to the network.
     * @param ex          experienced.
     * @param destination to which the send was addressed.
     */
    public static void sendError(final int bytesToSend, final IOException ex, final InetSocketAddress destination)
    {
        throw new AeronException("failed to send packet of " + bytesToSend + " bytes to " + destination, ex);
    }

    /**
     * Create the underlying channel for reading and writing.
     *
     * @param statusIndicator to set for error status
     */
    public void openDatagramChannel(final AtomicCounter statusIndicator)
    {
        try
        {
            sendDatagramChannel = DatagramChannel.open(udpChannel.protocolFamily());
            receiveDatagramChannel = sendDatagramChannel;

            if (udpChannel.isMulticast())
            {
                if (null != connectAddress)
                {
                    receiveDatagramChannel = DatagramChannel.open(udpChannel.protocolFamily());
                }

                receiveDatagramChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                receiveDatagramChannel.bind(new InetSocketAddress(endPointAddress.getPort()));
                receiveDatagramChannel.join(endPointAddress.getAddress(), udpChannel.localInterface());
                sendDatagramChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, udpChannel.localInterface());

                if (0 != udpChannel.multicastTtl())
                {
                    sendDatagramChannel.setOption(StandardSocketOptions.IP_MULTICAST_TTL, udpChannel.multicastTtl());
                    multicastTtl = sendDatagramChannel.getOption(StandardSocketOptions.IP_MULTICAST_TTL);
                }
            }
            else
            {
                sendDatagramChannel.bind(bindAddress);
            }

            if (null != connectAddress)
            {
                sendDatagramChannel.connect(connectAddress);
            }

            if (0 != Configuration.SOCKET_SNDBUF_LENGTH)
            {
                sendDatagramChannel.setOption(SO_SNDBUF, Configuration.SOCKET_SNDBUF_LENGTH);
            }

            if (0 != Configuration.SOCKET_RCVBUF_LENGTH)
            {
                receiveDatagramChannel.setOption(SO_RCVBUF, Configuration.SOCKET_RCVBUF_LENGTH);
            }

            sendDatagramChannel.configureBlocking(false);
            receiveDatagramChannel.configureBlocking(false);
        }
        catch (final IOException ex)
        {
            if (null != statusIndicator)
            {
                statusIndicator.setOrdered(ChannelEndpointStatus.ERRORED);
            }

            CloseHelper.quietClose(sendDatagramChannel);
            if (receiveDatagramChannel != sendDatagramChannel)
            {
                CloseHelper.quietClose(receiveDatagramChannel);
            }

            sendDatagramChannel = null;
            receiveDatagramChannel = null;

            throw new AeronException(
                "channel error - " + ex.getMessage() +
                " (at " + ex.getStackTrace()[0].toString() + "): " +
                udpChannel.originalUriString(), ex);
        }
    }

    /**
     * Register this transport for reading from a {@link UdpTransportPoller}.
     *
     * @param transportPoller to register read with
     */
    public void registerForRead(final UdpTransportPoller transportPoller)
    {
        this.transportPoller = transportPoller;
        selectionKey = transportPoller.registerForRead(this);
    }

    /**
     * Return underlying {@link UdpChannel}
     *
     * @return underlying channel
     */
    public UdpChannel udpChannel()
    {
        return udpChannel;
    }

    /**
     * The {@link DatagramChannel} for this transport channel.
     *
     * @return {@link DatagramChannel} for this transport channel.
     */
    public DatagramChannel receiveDatagramChannel()
    {
        return receiveDatagramChannel;
    }

    /**
     * Get the multicast TTL value for sending datagrams on the channel.
     *
     * @return the multicast TTL value for sending datagrams on the channel.
     */
    public int multicastTtl()
    {
        return multicastTtl;
    }

    /**
     * Close transport, canceling any pending read operations and closing channel
     */
    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            try
            {
                if (null != selectionKey)
                {
                    selectionKey.cancel();
                }

                if (null != transportPoller)
                {
                    transportPoller.cancelRead(this);
                    transportPoller.selectNowWithoutProcessing();
                }

                if (null != sendDatagramChannel)
                {
                    sendDatagramChannel.close();
                }

                if (receiveDatagramChannel != sendDatagramChannel && null != receiveDatagramChannel)
                {
                    receiveDatagramChannel.close();
                }

                if (null != transportPoller)
                {
                    transportPoller.selectNowWithoutProcessing();
                }
            }
            catch (final IOException ex)
            {
                errorLog.record(ex);
            }
        }
    }

    /**
     * Is transport representing a multicast media or unicast
     *
     * @return if transport is multicast media
     */
    public boolean isMulticast()
    {
        return udpChannel.isMulticast();
    }

    /**
     * Is the received frame valid. This method will do some basic checks on the header and can be
     * overridden in a subclass for further validation.
     *
     * @param buffer containing the frame.
     * @param length of the frame.
     * @return true if the frame is believed valid otherwise false.
     */
    public boolean isValidFrame(final UnsafeBuffer buffer, final int length)
    {
        boolean isFrameValid = true;

        if (frameVersion(buffer, 0) != HeaderFlyweight.CURRENT_VERSION)
        {
            isFrameValid = false;
            invalidPackets.increment();
        }
        else if (length < HeaderFlyweight.HEADER_LENGTH)
        {
            isFrameValid = false;
            invalidPackets.increment();
        }

        return isFrameValid;
    }

    @SuppressWarnings("unused")
    public void sendHook(final ByteBuffer buffer, final InetSocketAddress address)
    {
    }

    @SuppressWarnings("unused")
    public void receiveHook(final UnsafeBuffer buffer, final int length, final InetSocketAddress address)
    {
    }

    /**
     * Receive a datagram from the media layer.
     *
     * @param buffer into which the datagram will be received.
     * @return the source address of the datagram if one is available otherwise false.
     */
    public InetSocketAddress receive(final ByteBuffer buffer)
    {
        buffer.clear();

        InetSocketAddress address = null;
        try
        {
            if (receiveDatagramChannel.isOpen())
            {
                address = (InetSocketAddress)receiveDatagramChannel.receive(buffer);
            }
        }
        catch (final PortUnreachableException ignored)
        {
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return address;
    }
}

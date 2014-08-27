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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
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
public final class UdpTransport implements AutoCloseable
{
    private final DatagramChannel datagramChannel;
    private final UdpChannel udpChannel;

    private final ByteBuffer readByteBuffer = ByteBuffer.allocateDirect(Configuration.READ_BYTE_BUFFER_SZ);
    private final AtomicBuffer readBuffer = new AtomicBuffer(readByteBuffer);

    private final HeaderFlyweight header = new HeaderFlyweight();
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final NakFlyweight nakHeader = new NakFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();
    private final SetupFlyweight setupHeader = new SetupFlyweight();

    private final DataFrameHandler dataFrameHandler;
    private final StatusMessageFrameHandler smFrameHandler;
    private final NakFrameHandler nakFrameHandler;
    private final SetupFrameHandler setupFrameHandler;

    private final LossGenerator dataLossGenerator;
    private final LossGenerator controlLossGenerator;

    private final EventLogger logger;
    private final boolean multicast;
    private SelectionKey registeredKey;

    /**
     * Construct a transport for use with receiving and processing data frames
     *
     * @param udpChannel       of the transport
     * @param dataFrameHandler to call when data frames are received
     * @param logger           for logging
     * @param lossGenerator    for loss generation
     */
    public UdpTransport(
        final UdpChannel udpChannel,
        final DataFrameHandler dataFrameHandler,
        final SetupFrameHandler setupFrameHandler,
        final EventLogger logger,
        final LossGenerator lossGenerator)
    {
        this(
            udpChannel,
            dataFrameHandler,
            setupFrameHandler,
            null,
            null,
            logger,
            udpChannel.remoteData(),
            udpChannel.remoteData(),
            lossGenerator,
            null);
    }

    /**
     * Construct a transport for use with receiving and processing control frames
     *
     * Does not register
     *
     * @param udpChannel      of the transport
     * @param smFrameHandler  to call when status message frames are received
     * @param nakFrameHandler to call when NAK frames are received
     * @param logger          for logging
     * @param lossGenerator   for loss generation
     */
    public UdpTransport(
        final UdpChannel udpChannel,
        final StatusMessageFrameHandler smFrameHandler,
        final NakFrameHandler nakFrameHandler,
        final EventLogger logger,
        final LossGenerator lossGenerator)
    {
        this(
            udpChannel,
            null,
            null,
            smFrameHandler,
            nakFrameHandler,
            logger,
            udpChannel.remoteControl(),
            udpChannel.localControl(),
            null,
            lossGenerator);
    }

    private UdpTransport(
        final UdpChannel udpChannel,
        final DataFrameHandler dataFrameHandler,
        final SetupFrameHandler setupFrameHandler,
        final StatusMessageFrameHandler smFrameHandler,
        final NakFrameHandler nakFrameHandler,
        final EventLogger logger,
        final InetSocketAddress endPointSocketAddress,
        final InetSocketAddress bindAddress,
        final LossGenerator dataLossGenerator,
        final LossGenerator controlLossGenerator)
    {
        this.udpChannel = udpChannel;
        this.logger = logger;
        this.dataFrameHandler = dataFrameHandler;
        this.setupFrameHandler = setupFrameHandler;
        this.smFrameHandler = smFrameHandler;
        this.nakFrameHandler = nakFrameHandler;
        this.dataLossGenerator = dataLossGenerator;
        this.controlLossGenerator = controlLossGenerator;

        header.wrap(readBuffer, 0);
        dataHeader.wrap(readBuffer, 0);
        nakHeader.wrap(readBuffer, 0);
        statusMessage.wrap(readBuffer, 0);
        setupHeader.wrap(readBuffer, 0);

        try
        {
            datagramChannel = DatagramChannel.open();
            if (udpChannel.isMulticast())
            {
                final InetAddress endPointAddress = endPointSocketAddress.getAddress();
                final int dstPort = endPointSocketAddress.getPort();
                final NetworkInterface localInterface = udpChannel.localInterface();

                datagramChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                datagramChannel.bind(new InetSocketAddress(dstPort));
                datagramChannel.join(endPointAddress, localInterface);
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

            datagramChannel.configureBlocking(false);

        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public UdpChannel udpChannel()
    {
        return udpChannel;
    }

    /**
     * Send contents of {@link ByteBuffer} to remote address
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
     * Register transport with {@link NioSelector} for reading from the channel
     *
     * @param nioSelector to register read with
     */
    public void registerForRead(final NioSelector nioSelector)
    {
        if (null != dataFrameHandler)
        {
            registeredKey = nioSelector.registerForRead(datagramChannel, this::onReadDataFrames);
        }
        else
        {
            registeredKey = nioSelector.registerForRead(datagramChannel, this::onReadControlFrames);
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

    private int onReadDataFrames()
    {
        final InetSocketAddress srcAddress = receiveFrame();
        final int length = readByteBuffer.position();

        if (null == srcAddress)
        {
            return 0;
        }

        if (dataLossGenerator.shouldDropFrame(srcAddress, length))
        {
            logger.log(EventCode.FRAME_IN_DROPPED, readByteBuffer, 0, readByteBuffer.position(), srcAddress);
            return 0;
        }

        logger.log(EventCode.FRAME_IN, readByteBuffer, 0, readByteBuffer.position(), srcAddress);

        if (!isValidFrame(length))
        {
            return 0;
        }

        switch (header.headerType())
        {
            case HDR_TYPE_PAD:
            case HDR_TYPE_DATA:
                dataFrameHandler.onFrame(dataHeader, readBuffer, length, srcAddress);
                return 1;

            case HDR_TYPE_SETUP:
                setupFrameHandler.onFrame(setupHeader, readBuffer, length, srcAddress);
                return 1;
        }

        return 0;
    }

    private int onReadControlFrames()
    {
        final InetSocketAddress srcAddress = receiveFrame();
        final int length = readByteBuffer.position();

        if (null == srcAddress)
        {
            return 0;
        }

        if (controlLossGenerator.shouldDropFrame(srcAddress, length))
        {
            logger.log(EventCode.FRAME_IN_DROPPED, readByteBuffer, 0, readByteBuffer.position(), srcAddress);
            return 0;
        }

        logger.log(EventCode.FRAME_IN, readByteBuffer, 0, readByteBuffer.position(), srcAddress);

        if (!isValidFrame(length))
        {
            return 0;
        }

        switch (header.headerType())
        {
            case HDR_TYPE_NAK:
                nakFrameHandler.onFrame(nakHeader, readBuffer, length, srcAddress);
                return 1;

            case HDR_TYPE_SM:
                smFrameHandler.onFrame(statusMessage, readBuffer, length, srcAddress);
                return 1;
        }

        return 0;
    }

    private InetSocketAddress receiveFrame()
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

    private boolean isValidFrame(final int length)
    {
        if (header.version() != HeaderFlyweight.CURRENT_VERSION)
        {
            logger.log(EventCode.MALFORMED_FRAME_LENGTH, readBuffer, 0, header.frameLength());
            return false;
        }

        if (length <= FrameDescriptor.BASE_HEADER_LENGTH)
        {
            logger.log(EventCode.MALFORMED_FRAME_LENGTH, readBuffer, 0, length);
            return false;
        }

        return true;
    }
}

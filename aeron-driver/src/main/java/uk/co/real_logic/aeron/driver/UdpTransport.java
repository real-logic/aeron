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
public final class UdpTransport implements AutoCloseable
{
    @FunctionalInterface
    public interface DataFrameHandler
    {
        /**
         * Handle a Data Frame.
         *
         * @param header of the first Data Frame in the message (may be re-wrapped if needed)
         * @param buffer holding the data (always starts at 0 offset)
         * @param length of the Frame (may be longer than the header frame length)
         * @param srcAddress of the Frame
         */
        void onFrame(final DataHeaderFlyweight header, final AtomicBuffer buffer,
                     final int length, final InetSocketAddress srcAddress);
    }

    @FunctionalInterface
    public interface StatusMessageFrameHandler
    {
        /**
         * Handle a Status Message Frame
         *
         * @param header of the first Status Message Frame in the message (may be re-wrapped if needed)
         * @param buffer holding the NAK (always starts at 0 offset)
         * @param length of the Frame (may be longer than the header frame length)
         * @param srcAddress of the Frame
         */
        void onFrame(final StatusMessageFlyweight header, final AtomicBuffer buffer,
                     final int length, final InetSocketAddress srcAddress);
    }

    @FunctionalInterface
    public interface NakFrameHandler
    {
        /**
         * Handle a NAK Frame
         *
         * @param header the first NAK Frame in the message (may be re-wrapped if needed)
         * @param buffer holding the Status Message (always starts at 0 offset)
         * @param length of the Frame (may be longer than the header frame length)
         * @param srcAddress of the Frame
         */
        void onFrame(final NakFlyweight header, final AtomicBuffer buffer,
                     final int length, final InetSocketAddress srcAddress);
    }

    private final DatagramChannel channel = DatagramChannel.open();

    private final ByteBuffer readByteBuffer = ByteBuffer.allocateDirect(MediaDriver.READ_BYTE_BUFFER_SZ);
    private final AtomicBuffer readBuffer = new AtomicBuffer(readByteBuffer);

    private final HeaderFlyweight header = new HeaderFlyweight();
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final NakFlyweight nakHeader = new NakFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

    private final DataFrameHandler dataFrameHandler;
    private final StatusMessageFrameHandler smFrameHandler;
    private final NakFrameHandler nakFrameHandler;

    private final EventLogger logger;

    private final boolean multicast;

    private SelectionKey registeredKey;

    /**
     * Construct a transport for use with receiving and processing data frames
     *
     * @param destination of the transport
     * @param dataFrameHandler to call when data frames are received
     * @param logger for logging
     * @throws Exception
     */
    public UdpTransport(final UdpDestination destination,
                        final DataFrameHandler dataFrameHandler,
                        final EventLogger logger)
        throws Exception
    {
        this(destination, dataFrameHandler, null, null, logger, destination.remoteData(),
            destination.remoteData());
    }

    /**
     * Construct a transport for use with receiving and processing control frames
     *
     * Does not register
     *
     * @param destination of the transport
     * @param smFrameHandler to call when status message frames are received
     * @param nakFrameHandler to call when NAK frames are received
     * @param logger for logging
     * @throws Exception
     */
    public UdpTransport(final UdpDestination destination,
                        final StatusMessageFrameHandler smFrameHandler,
                        final NakFrameHandler nakFrameHandler,
                        final EventLogger logger)
        throws Exception
    {
        this(destination, null, smFrameHandler, nakFrameHandler, logger, destination.remoteControl(),
            destination.localControl());
    }

    private UdpTransport(final UdpDestination destination,
                         final DataFrameHandler dataFrameHandler,
                         final StatusMessageFrameHandler smFrameHandler,
                         final NakFrameHandler nakFrameHandler,
                         final EventLogger logger,
                         final InetSocketAddress endPointSocketAddress,
                         final InetSocketAddress bindAddress)
        throws Exception
    {
        this.logger = logger;
        this.dataFrameHandler = dataFrameHandler;
        this.smFrameHandler = smFrameHandler;
        this.nakFrameHandler = nakFrameHandler;

        header.wrap(readBuffer, 0);
        dataHeader.wrap(readBuffer, 0);
        nakHeader.wrap(readBuffer, 0);
        statusMessage.wrap(readBuffer, 0);

        if (destination.isMulticast())
        {
            final InetAddress endPointAddress = endPointSocketAddress.getAddress();
            final int dstPort = endPointSocketAddress.getPort();
            final NetworkInterface localInterface = destination.localInterface();

            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            channel.bind(new InetSocketAddress(dstPort));
            channel.join(endPointAddress, localInterface);
            channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, localInterface);
            multicast = true;
        }
        else
        {
            channel.bind(bindAddress);
            multicast = false;
        }

        channel.configureBlocking(false);
    }

    /**
     * Send contents of {@link ByteBuffer} to remote address
     *
     * @param buffer to send
     * @param remoteAddress to send to
     * @return number of bytes sent
     * @throws Exception
     */
    public int sendTo(final ByteBuffer buffer, final InetSocketAddress remoteAddress) throws Exception
    {
        logger.log(EventCode.FRAME_OUT, buffer, buffer.remaining(), remoteAddress);

        return channel.send(buffer, remoteAddress);
    }

    /**
     * Register transport with {@link NioSelector} for reading from the channel
     *
     * @param nioSelector to register read with
     * @throws Exception
     */
    public void registerForRead(final NioSelector nioSelector) throws Exception
    {
        if (null != dataFrameHandler)
        {
            registeredKey = nioSelector.registerForRead(channel, this::onReadDataFrames);
        }
        else
        {
            registeredKey = nioSelector.registerForRead(channel, this::onReadControlFrames);
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

            channel.close();
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

        if (null == srcAddress)
        {
            return 0;
        }

        final int length = readByteBuffer.position();

        if (!isValidFrame(length))
        {
            return 0;
        }

        if (header.headerType() == HeaderFlyweight.HDR_TYPE_DATA)
        {
            dataFrameHandler.onFrame(dataHeader, readBuffer, length, srcAddress);
            return 1;
        }

//        logger.log(EventCode.UNKNOWN_HEADER_TYPE, readBuffer, 0, HeaderFlyweight.HEADER_LENGTH);
        return 0;
    }

    private int onReadControlFrames()
    {
        final InetSocketAddress srcAddress = receiveFrame();

        if (null == srcAddress)
        {
            return 0;
        }

        final int length = readByteBuffer.position();

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

            default:
//                logger.log(EventCode.UNKNOWN_HEADER_TYPE, readBuffer, 0, HeaderFlyweight.HEADER_LENGTH);
                break;
        }

        return 0;
    }

    private InetSocketAddress receiveFrame()
    {
        readByteBuffer.clear();

        try
        {
            final InetSocketAddress srcAddress = (InetSocketAddress) channel.receive(readByteBuffer);

            if (null != srcAddress)
            {
                logger.log(EventCode.FRAME_IN, readByteBuffer, readByteBuffer.position(), srcAddress);
            }

            return srcAddress;
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private boolean isValidFrame(final int length)
    {
        // drop a version we don't know
        if (header.version() != HeaderFlyweight.CURRENT_VERSION)
        {
            logger.log(EventCode.MALFORMED_FRAME_LENGTH, readBuffer, 0, header.frameLength());
            return false;
        }

        // too short
        if (length <= FrameDescriptor.BASE_HEADER_LENGTH)
        {
            logger.log(EventCode.MALFORMED_FRAME_LENGTH, readBuffer, 0, length);
            return false;
        }

        return true;
    }
}

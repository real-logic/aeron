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

import uk.co.real_logic.aeron.common.event.EventCode;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.SetupFlyweight;

import java.net.InetSocketAddress;

import static uk.co.real_logic.aeron.common.protocol.HeaderFlyweight.*;

/**
 * Transport abstraction for UDP sources and receivers.
 *
 * We don't conflate the processing logic, or we at least try not to, into this object.
 *
 * Holds DatagramChannel, read Buffer, etc.
 */
public final class ReceiverUdpChannelTransport extends UdpChannelTransport
{
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final SetupFlyweight setupHeader = new SetupFlyweight();

    private final DataFrameHandler dataFrameHandler;
    private final SetupFrameHandler setupFrameHandler;

    /**
     * Construct a transport for use with receiving and processing data frames
     *
     * @param udpChannel       of the transport
     * @param dataFrameHandler to call when data frames are received
     * @param logger           for logging
     * @param lossGenerator    for loss generation
     */
    public ReceiverUdpChannelTransport(
        final UdpChannel udpChannel,
        final DataFrameHandler dataFrameHandler,
        final SetupFrameHandler setupFrameHandler,
        final EventLogger logger,
        final LossGenerator lossGenerator)
    {
        super(udpChannel, udpChannel.remoteData(), udpChannel.remoteData(), lossGenerator, logger);

        this.dataFrameHandler = dataFrameHandler;
        this.setupFrameHandler = setupFrameHandler;

        dataHeader.wrap(receiveBuffer, 0);
        setupHeader.wrap(receiveBuffer, 0);
    }

    /**
     * Register transport with {@link NioSelector} for reading from the channel
     *
     * @param nioSelector to register read with
     */
    public void registerForRead(final NioSelector nioSelector)
    {
        registeredNioSelector = nioSelector;
        registeredKey = nioSelector.registerForRead(datagramChannel, this);
    }

    /**
     * Callback for processing frames.
     *
     * @return the number of frames processed.
     */
    public int attemptReceive()
    {
        int framesRead = 0;
        final InetSocketAddress srcAddress = receiveFrame();

        if (null != srcAddress)
        {
            final int length = receiveByteBuffer.position();
            if (lossGenerator.shouldDropFrame(srcAddress, length))
            {
                logger.log(EventCode.FRAME_IN_DROPPED, receiveByteBuffer, 0, length, srcAddress);
            }
            else
            {
                logger.log(EventCode.FRAME_IN, receiveByteBuffer, 0, length, srcAddress);

                if (isFrameValid(length))
                {
                    switch (header.headerType())
                    {
                        case HDR_TYPE_PAD:
                        case HDR_TYPE_DATA:
                            framesRead = dataFrameHandler.onFrame(dataHeader, receiveBuffer, length, srcAddress);
                            break;

                        case HDR_TYPE_SETUP:
                            setupFrameHandler.onFrame(setupHeader, receiveBuffer, length, srcAddress);
                            break;
                    }
                }
            }
        }

        return framesRead;
    }
}

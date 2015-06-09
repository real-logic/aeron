/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver.media;

import uk.co.real_logic.aeron.driver.event.EventLogger;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.protocol.SetupFlyweight;
import uk.co.real_logic.aeron.driver.DataPacketHandler;
import uk.co.real_logic.aeron.driver.LossGenerator;
import uk.co.real_logic.aeron.driver.SetupMessageHandler;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.frameType;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.*;

/**
 * Transport abstraction for UDP send and receive from the media
 *
 * We don't conflate the processing logic, or we at least try not to, into this object.
 *
 * Holds DatagramChannel, read Buffer, etc.
 */
public final class ReceiverUdpChannelTransport extends UdpChannelTransport
{
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final SetupFlyweight setupHeader = new SetupFlyweight();

    private final DataPacketHandler dataPacketHandler;
    private final SetupMessageHandler setupMessageHandler;

    /**
     * Construct a transport for use with receiving and processing data frames
     *
     * @param udpChannel        of the transport
     * @param dataPacketHandler to call when data frames are received
     * @param logger            for logging
     * @param lossGenerator     for loss generation
     */
    public ReceiverUdpChannelTransport(
        final UdpChannel udpChannel,
        final DataPacketHandler dataPacketHandler,
        final SetupMessageHandler setupMessageHandler,
        final EventLogger logger,
        final LossGenerator lossGenerator)
    {
        super(udpChannel, udpChannel.remoteData(), udpChannel.remoteData(), lossGenerator, logger);

        this.dataPacketHandler = dataPacketHandler;
        this.setupMessageHandler = setupMessageHandler;

        dataHeader.wrap(receiveBuffer(), 0);
        setupHeader.wrap(receiveBuffer(), 0);
    }

    protected int dispatch(final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
    {
        int bytesReceived = 0;
        switch (frameType(buffer, 0))
        {
            case HDR_TYPE_PAD:
            case HDR_TYPE_DATA:
                bytesReceived = dataPacketHandler.onDataPacket(dataHeader, buffer, length, srcAddress);
                break;

            case HDR_TYPE_SETUP:
                setupMessageHandler.onSetupMessage(setupHeader, buffer, length, srcAddress);
                break;
        }

        return bytesReceived;
    }
}

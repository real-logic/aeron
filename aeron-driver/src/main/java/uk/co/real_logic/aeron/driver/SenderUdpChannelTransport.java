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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.NakFlyweight;
import uk.co.real_logic.aeron.common.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;

import static uk.co.real_logic.aeron.common.protocol.HeaderFlyweight.HDR_TYPE_NAK;
import static uk.co.real_logic.aeron.common.protocol.HeaderFlyweight.HDR_TYPE_SM;

/**
 * Transport abstraction for UDP send and receive from the media
 *
 * We don't conflate the processing logic, or we at least try not to, into this object.
 *
 * Holds DatagramChannel, read Buffer, etc.
 */
public final class SenderUdpChannelTransport extends UdpChannelTransport
{
    private final NakFlyweight nakHeader = new NakFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

    private final StatusMessageFrameHandler smFrameHandler;
    private final NakFrameHandler nakFrameHandler;

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
    public SenderUdpChannelTransport(
        final UdpChannel udpChannel,
        final StatusMessageFrameHandler smFrameHandler,
        final NakFrameHandler nakFrameHandler,
        final EventLogger logger,
        final LossGenerator lossGenerator)
    {
        super(udpChannel, udpChannel.remoteControl(), udpChannel.localControl(), lossGenerator, logger);

        this.smFrameHandler = smFrameHandler;
        this.nakFrameHandler = nakFrameHandler;

        nakHeader.wrap(receiveBuffer(), 0);
        statusMessage.wrap(receiveBuffer(), 0);
    }

    protected int dispatch(
        final int headerType, final UnsafeBuffer receiveBuffer, final int length, final InetSocketAddress srcAddress)
    {
        int framesRead = 0;
        switch (headerType)
        {
            case HDR_TYPE_NAK:
                nakFrameHandler.onFrame(nakHeader, receiveBuffer, length, srcAddress);
                framesRead = 1;
                break;

            case HDR_TYPE_SM:
                smFrameHandler.onFrame(statusMessage, receiveBuffer, length, srcAddress);
                framesRead = 1;
                break;
        }

        return framesRead;
    }
}

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
import uk.co.real_logic.aeron.protocol.NakFlyweight;
import uk.co.real_logic.aeron.protocol.StatusMessageFlyweight;
import uk.co.real_logic.aeron.driver.LossGenerator;
import uk.co.real_logic.aeron.driver.NakMessageHandler;
import uk.co.real_logic.aeron.driver.StatusMessageHandler;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.frameType;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.HDR_TYPE_NAK;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.HDR_TYPE_SM;

/**
 * Transport abstraction for UDP send and receive from the media
 *
 * We don't conflate the processing logic, or we at least try not to, into this object.
 *
 * Holds DatagramChannel, read Buffer, etc.
 */
public final class SenderUdpChannelTransport extends UdpChannelTransport
{
    private final NakFlyweight nakMessage = new NakFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

    private final StatusMessageHandler smMessageHandler;
    private final NakMessageHandler nakMessageHandler;

    /**
     * Construct a transport for use with receiving and processing control frames
     *
     * Does not register
     *
     * @param udpChannel        of the transport
     * @param smMessageHandler  to call when status message frames are received
     * @param nakMessageHandler to call when NAK frames are received
     * @param logger            for logging
     * @param lossGenerator     for loss generation
     */
    public SenderUdpChannelTransport(
        final UdpChannel udpChannel,
        final StatusMessageHandler smMessageHandler,
        final NakMessageHandler nakMessageHandler,
        final EventLogger logger,
        final LossGenerator lossGenerator)
    {
        super(udpChannel, udpChannel.remoteControl(), udpChannel.localControl(), lossGenerator, logger);

        this.smMessageHandler = smMessageHandler;
        this.nakMessageHandler = nakMessageHandler;

        nakMessage.wrap(receiveBuffer(), 0);
        statusMessage.wrap(receiveBuffer(), 0);
    }

    protected int dispatch(final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
    {
        int framesRead = 0;
        switch (frameType(buffer, 0))
        {
            case HDR_TYPE_NAK:
                nakMessageHandler.onMessage(nakMessage);
                framesRead = 1;
                break;

            case HDR_TYPE_SM:
                smMessageHandler.onMessage(statusMessage, srcAddress);
                framesRead = 1;
                break;
        }

        return framesRead;
    }
}

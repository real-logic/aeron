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
import uk.co.real_logic.aeron.common.protocol.NakFlyweight;
import uk.co.real_logic.aeron.common.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.util.function.IntSupplier;

import static uk.co.real_logic.aeron.common.protocol.HeaderFlyweight.*;

/**
 * Transport abstraction for UDP sources and receivers.
 *
 * We don't conflate the processing logic, or we at least try not to, into this object.
 *
 * Holds DatagramChannel, read Buffer, etc.
 */
public final class SenderUdpChannelTransport extends UdpChannelTransport implements IntSupplier
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

        nakHeader.wrap(readBuffer, 0);
        statusMessage.wrap(readBuffer, 0);
    }

    /**
     * Register this transport for reading from a {@link NioSelector}.
     *
     * @param nioSelector to register read with
     */
    public void registerForRead(final NioSelector nioSelector)
    {
        registeredKey = nioSelector.registerForRead(datagramChannel, this);
    }

    /**
     * Implementation of {@link IntSupplier#getAsInt()} to be used as callback for processing frames.
     *
     * @return the number of frames processed.
     */
    public int getAsInt()
    {
        return receiveControlFrames();
    }

    private int receiveControlFrames()
    {
        int framesRead = 0;
        final InetSocketAddress srcAddress = receiveFrame();

        if (null != srcAddress)
        {
            final int length = readByteBuffer.position();
            if (lossGenerator.shouldDropFrame(srcAddress, length))
            {
                logger.log(EventCode.FRAME_IN_DROPPED, readByteBuffer, 0, readByteBuffer.position(), srcAddress);
            }
            else
            {
                logger.log(EventCode.FRAME_IN, readByteBuffer, 0, readByteBuffer.position(), srcAddress);

                if (isFrameValid(length))
                {
                    switch (header.headerType())
                    {
                        case HDR_TYPE_NAK:
                            nakFrameHandler.onFrame(nakHeader, readBuffer, length, srcAddress);
                            framesRead = 1;
                            break;

                        case HDR_TYPE_SM:
                            smFrameHandler.onFrame(statusMessage, readBuffer, length, srcAddress);
                            framesRead = 1;
                            break;
                    }
                }
            }
        }

        return framesRead;
    }
}

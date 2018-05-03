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

import io.aeron.driver.DataPacketDispatcher;
import io.aeron.driver.MediaDriver;
import org.agrona.LangUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.driver.status.SystemCounterDescriptor.INVALID_PACKETS;

public class ReceiveDestinationUdpTransport extends UdpChannelTransport
{
    private final Long2ObjectHashMap<InetSocketAddress> controlAddressMap = new Long2ObjectHashMap<>();

    public ReceiveDestinationUdpTransport(
        final UdpChannel udpChannel,
        final DataPacketDispatcher dispatcher,
        final AtomicCounter statusIndicator,
        final MediaDriver.Context context)
    {
        super(
            udpChannel,
            udpChannel.remoteData(),
            udpChannel.remoteData(),
            null,
            context.errorLog(),
            context.systemCounters().get(INVALID_PACKETS));
    }

    public void addControlAddress(final long imageCorrelationId, final InetSocketAddress controlAddress)
    {
        controlAddressMap.put(imageCorrelationId, controlAddress);
    }

    public void removeControlAddress(final long imageCorrelationId)
    {
        controlAddressMap.remove(imageCorrelationId);
    }

    public int sendTo(final ByteBuffer buffer, final long imageCorrelationId)
    {
        final InetSocketAddress controlAddress = controlAddressMap.get(imageCorrelationId);
        int bytesSent = 0;
        try
        {
            if (null != sendDatagramChannel && null != controlAddress)
            {
                bytesSent = sendDatagramChannel.send(buffer, controlAddress);
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return bytesSent;
    }
}

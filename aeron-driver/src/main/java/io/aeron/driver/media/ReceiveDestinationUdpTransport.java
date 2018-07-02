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

import io.aeron.driver.MediaDriver;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;

import static io.aeron.driver.status.SystemCounterDescriptor.INVALID_PACKETS;

public class ReceiveDestinationUdpTransport extends UdpChannelTransport
{
    public ReceiveDestinationUdpTransport(
        final UdpChannel udpChannel,
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

    public void openChannel()
    {
        openDatagramChannel(null);
    }

    public boolean hasExplicitControl()
    {
        return udpChannel.hasExplicitControl();
    }

    public InetSocketAddress explicitControlAddress()
    {
        return udpChannel.hasExplicitControl() ? udpChannel.localControl() : null;
    }

    public void selectionKey(final SelectionKey key)
    {
        selectionKey = key;
    }
}

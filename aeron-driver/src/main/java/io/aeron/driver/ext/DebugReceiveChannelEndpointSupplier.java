/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.driver.ext;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.DataPacketDispatcher;
import io.aeron.driver.ReceiveChannelEndpointSupplier;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Supply a debug implementation of a {@link ReceiveChannelEndpoint}.
 */
public class DebugReceiveChannelEndpointSupplier implements ReceiveChannelEndpointSupplier
{
    public ReceiveChannelEndpoint newInstance(
        final UdpChannel udpChannel,
        final DataPacketDispatcher dispatcher,
        final AtomicCounter statusIndicator,
        final MediaDriver.Context context)
    {
        return new DebugReceiveChannelEndpoint(udpChannel, dispatcher, statusIndicator, context);
    }
}

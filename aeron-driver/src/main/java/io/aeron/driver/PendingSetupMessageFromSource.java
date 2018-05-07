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
package io.aeron.driver;

import io.aeron.driver.media.ReceiveChannelEndpoint;

import java.net.InetSocketAddress;

class PendingSetupMessageFromSource
{
    private final int sessionId;
    private final int streamId;
    private final int transportIndex;
    private final boolean periodic;
    private final ReceiveChannelEndpoint channelEndpoint;
    private final InetSocketAddress controlAddress;

    private long timeOfStatusMessageNs;

    PendingSetupMessageFromSource(
        final int sessionId,
        final int streamId,
        final int transportIndex,
        final ReceiveChannelEndpoint channelEndpoint,
        final boolean periodic,
        final InetSocketAddress controlAddress)
    {
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.transportIndex = transportIndex;
        this.channelEndpoint = channelEndpoint;
        this.periodic = periodic;
        this.controlAddress = controlAddress;
    }

    public int sessionId()
    {
        return sessionId;
    }

    public int streamId()
    {
        return streamId;
    }

    public int transportIndex()
    {
        return transportIndex;
    }

    public ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    public boolean isPeriodic()
    {
        return periodic;
    }

    public boolean shouldElicitSetupMessage()
    {
        return channelEndpoint.shouldElicitSetupMessage();
    }

    public InetSocketAddress controlAddress()
    {
        return controlAddress;
    }

    public long timeOfStatusMessageNs()
    {
        return timeOfStatusMessageNs;
    }

    public void timeOfStatusMessageNs(final long now)
    {
        timeOfStatusMessageNs = now;
    }

    public void removeFromDataPacketDispatcher()
    {
        channelEndpoint.removePendingSetup(sessionId, streamId);
    }
}

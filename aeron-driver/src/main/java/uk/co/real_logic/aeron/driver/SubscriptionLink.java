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

import uk.co.real_logic.aeron.driver.media.ReceiveChannelEndpoint;
import uk.co.real_logic.agrona.concurrent.status.ReadablePosition;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Subscription registration from a client used for liveness tracking
 */
public class SubscriptionLink
{
    private final long registrationId;
    private final int streamId;
    private final ReceiveChannelEndpoint channelEndpoint;
    private final AeronClient aeronClient;
    private final Map<NetworkConnection, ReadablePosition> positionByConnectionMap = new IdentityHashMap<>();

    public SubscriptionLink(
        final long registrationId,
        final ReceiveChannelEndpoint channelEndpoint,
        final int streamId,
        final AeronClient aeronClient)
    {
        this.registrationId = registrationId;
        this.channelEndpoint = channelEndpoint;
        this.streamId = streamId;
        this.aeronClient = aeronClient;
    }

    public long registrationId()
    {
        return registrationId;
    }

    public ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    public int streamId()
    {
        return streamId;
    }

    public long timeOfLastKeepaliveFromClient()
    {
        return aeronClient.timeOfLastKeepalive();
    }

    public boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        return channelEndpoint == this.channelEndpoint && streamId == this.streamId;
    }

    public void addConnection(final NetworkConnection connection, final ReadablePosition position)
    {
        positionByConnectionMap.put(connection, position);
    }

    public void removeConnection(final NetworkConnection connection)
    {
        positionByConnectionMap.remove(connection);
    }

    public void close()
    {
        positionByConnectionMap.forEach(NetworkConnection::removeSubscriber);
    }
}

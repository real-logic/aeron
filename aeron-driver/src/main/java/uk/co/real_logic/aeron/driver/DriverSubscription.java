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

import uk.co.real_logic.agrona.status.PositionIndicator;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Analogue of the client Subscription for the driver used for liveness tracking
 */
public class DriverSubscription
{
    private final long registrationId;
    private final ReceiveChannelEndpoint channelEndpoint;
    private final int streamId;
    private final AeronClient aeronClient;
    private final Map<DriverConnection, PositionIndicator> positionIndicatorByConnection = new IdentityHashMap<>();

    public DriverSubscription(
        final long registrationId,
        final ReceiveChannelEndpoint channelEndpoint,
        final AeronClient aeronClient,
        final int streamId)
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

    public ReceiveChannelEndpoint receiveChannelEndpoint()
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

    public boolean matches(final int streamId, final ReceiveChannelEndpoint channelEndpoint)
    {
        return streamId() == streamId && receiveChannelEndpoint() == channelEndpoint;
    }

    public void addConnection(final DriverConnection connection, final PositionIndicator positionIndicator)
    {
        positionIndicatorByConnection.put(connection, positionIndicator);
    }

    public void close()
    {
        positionIndicatorByConnection.forEach(DriverConnection::removeSubscription);
    }
}

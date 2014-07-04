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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;

import java.net.InetSocketAddress;

/**
 * Subscriptions maintained per channel for receiver processing
 */
public class DriverSubscription
{
    private final UdpDestination destination;
    private final long channelId;
    private final MediaConductorProxy conductorProxy;
    private final AtomicArray<DriverConnectedSubscription> connectedSubscriptions;
    private final Long2ObjectHashMap<DriverConnectedSubscription> connectionBySessionIdMap = new Long2ObjectHashMap<>();

    private int refCount = 0;

    public DriverSubscription(final UdpDestination destination,
                              final long channelId,
                              final MediaConductorProxy conductorProxy,
                              final AtomicArray<DriverConnectedSubscription> connectedSubscriptions)
    {
        this.destination = destination;
        this.channelId = channelId;
        this.conductorProxy = conductorProxy;
        this.connectedSubscriptions = connectedSubscriptions;
    }

    public int decRef()
    {
        return --refCount;
    }

    public int incRef()
    {
        return ++refCount;
    }

    public DriverConnectedSubscription getConnectedSubscription(final long sessionId)
    {
        return connectionBySessionIdMap.get(sessionId);
    }

    public DriverConnectedSubscription newConnectedSubscription(final long sessionId, final InetSocketAddress srcAddress)
    {
        final DriverConnectedSubscription connectedSubscription
            = new DriverConnectedSubscription(sessionId, channelId, srcAddress);
        connectedSubscriptions.add(connectedSubscription);

        return connectionBySessionIdMap.put(sessionId, connectedSubscription);
    }

    public long channelId()
    {
        return channelId;
    }

    public void close()
    {
        connectionBySessionIdMap.forEach(
            (sessionId, session) -> conductorProxy.removeTermBuffers(destination, sessionId, channelId)
        );
    }
}

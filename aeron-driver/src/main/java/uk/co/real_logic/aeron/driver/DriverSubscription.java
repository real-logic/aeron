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

import uk.co.real_logic.aeron.common.collections.Long2ObjectHashMap;

import java.util.Collection;

/**
 * Subscriptions maintained per channel for receiver processing
 */
public class DriverSubscription
{
    private final UdpDestination udpDestination;
    private final long channelId;
    private final DriverConductorProxy conductorProxy;
    private final Long2ObjectHashMap<DriverConnectedSubscription> connectionBySessionIdMap = new Long2ObjectHashMap<>();

    private int refCount = 0;

    public DriverSubscription(final UdpDestination udpDestination,
                              final long channelId,
                              final DriverConductorProxy conductorProxy)
    {
        this.udpDestination = udpDestination;
        this.channelId = channelId;
        this.conductorProxy = conductorProxy;
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

    public DriverConnectedSubscription putConnectedSubscription(final DriverConnectedSubscription connectedSubscription)
    {
        return connectionBySessionIdMap.put(connectedSubscription.sessionId(), connectedSubscription);
    }

    public long channelId()
    {
        return channelId;
    }

    public UdpDestination udpDestination()
    {
        return udpDestination;
    }

    public void close()
    {
        conductorProxy.removeSubscription(this);
    }

    public Collection<DriverConnectedSubscription> connectedSubscriptions()
    {
        return connectionBySessionIdMap.values();
    }
}

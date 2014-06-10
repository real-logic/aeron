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
public class Subscription
{
    private final UdpDestination destination;
    private final long channelId;
    private final MediaConductorProxy conductorProxy;
    private final AtomicArray<SubscribedSession> globallySubscribedSessions;
    private final Long2ObjectHashMap<SubscribedSession> subscribedSessionBySessionIdMap = new Long2ObjectHashMap<>();

    private int refCount = 1;

    public Subscription(final UdpDestination destination,
                        final long channelId,
                        final MediaConductorProxy conductorProxy,
                        final AtomicArray<SubscribedSession> globallySubscribedSessions)
    {
        this.destination = destination;
        this.channelId = channelId;
        this.conductorProxy = conductorProxy;
        this.globallySubscribedSessions = globallySubscribedSessions;
    }

    public int decRef()
    {
        return --refCount;
    }

    public int incRef()
    {
        return ++refCount;
    }

    public SubscribedSession getSubscribedSession(final long sessionId)
    {
        return subscribedSessionBySessionIdMap.get(sessionId);
    }

    public SubscribedSession createSubscribedSession(final long sessionId, final InetSocketAddress srcAddr)
    {
        final SubscribedSession subscribedSession = new SubscribedSession(sessionId, srcAddr);
        globallySubscribedSessions.add(subscribedSession);

        return subscribedSessionBySessionIdMap.put(sessionId, subscribedSession);
    }

    public long channelId()
    {
        return channelId;
    }

    public void close()
    {
        subscribedSessionBySessionIdMap.forEach(
            (sessionId, session) -> conductorProxy.removeRcvTermBuffer(destination, sessionId, channelId)
        );
    }
}

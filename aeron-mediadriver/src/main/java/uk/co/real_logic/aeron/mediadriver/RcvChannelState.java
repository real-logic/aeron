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
 * State maintained per channel for receiver processing
 */
public class RcvChannelState
{
    private final UdpDestination destination;
    private final long channelId;
    private final MediaConductorCursor adminThreadCursor;
    private final AtomicArray<RcvSessionState> allSessionState;
    private int referenceCount;
    private final Long2ObjectHashMap<RcvSessionState> sessionStateMap;

    public RcvChannelState(final UdpDestination destination,
                           final long channelId,
                           final MediaConductorCursor adminThreadCursor,
                           final AtomicArray<RcvSessionState> sessionState)
    {
        this.destination = destination;
        this.channelId = channelId;
        this.adminThreadCursor = adminThreadCursor;
        this.allSessionState = sessionState;
        this.referenceCount = 1;
        this.sessionStateMap = new Long2ObjectHashMap<>();
    }

    public int decrementReference()
    {
        return --referenceCount;
    }

    public int incrementReference()
    {
        return ++referenceCount;
    }

    public int referenceCount()
    {
        return referenceCount;
    }

    public RcvSessionState getSessionState(final long sessionId)
    {
        return sessionStateMap.get(sessionId);
    }

    public void removeSessionState(final long sessionId)
    {
        sessionStateMap.remove(sessionId);
    }

    public RcvSessionState createSessionState(final long sessionId, final InetSocketAddress srcAddr)
    {
        RcvSessionState sessionState = new RcvSessionState(sessionId, srcAddr);
        allSessionState.add(sessionState);
        return sessionStateMap.put(sessionId, sessionState);
    }

    public long channelId()
    {
        return channelId;
    }

    public void close()
    {
        sessionStateMap.forEach(
            (sessionId, session) ->
            {
                adminThreadCursor.addRemoveRcvTermBufferEvent(destination, sessionId, channelId);
            });
    }
}

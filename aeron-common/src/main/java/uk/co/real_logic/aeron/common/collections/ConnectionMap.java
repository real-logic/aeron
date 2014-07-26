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
package uk.co.real_logic.aeron.common.collections;

import java.util.HashMap;
import java.util.Map;

import static uk.co.real_logic.aeron.common.collections.CollectionUtil.getOrDefault;

/**
 * Map for storing information about Aeron associations. These are keyed
 * by a triple of channel/session/stream.
 */
public class ConnectionMap<D, C>
{
    private final Map<D, Int2ObjectHashMap<Int2ObjectHashMap<C>>> channelMap = new HashMap<>();

    public C get(final D channel, final int sessionId, final int streamId)
    {
        final Int2ObjectHashMap<Int2ObjectHashMap<C>> sessionMap = channelMap.get(channel);
        if (sessionMap == null)
        {
            return null;
        }

        final Int2ObjectHashMap<C> channelMap = sessionMap.get(sessionId);
        if (channelMap == null)
        {
            return null;
        }

        return channelMap.get(streamId);
    }

    public C put(final D channel, final int sessionId, final int streamId, final C value)
    {
        final Int2ObjectHashMap<Int2ObjectHashMap<C>> endPointMap
            = getOrDefault(channelMap, channel, ignore -> new Int2ObjectHashMap<>());
        final Int2ObjectHashMap<C> channelMap = endPointMap.getOrDefault(sessionId, Int2ObjectHashMap::new);

        return channelMap.put(streamId, value);
    }

    public C remove(final D channel, final int sessionId, final int streamId)
    {
        final Int2ObjectHashMap<Int2ObjectHashMap<C>> sessionMap = channelMap.get(channel);
        if (sessionMap == null)
        {
            return null;
        }

        final Int2ObjectHashMap<C> channelMap = sessionMap.get(sessionId);
        if (channelMap == null)
        {
            return null;
        }

        final C value = channelMap.remove(streamId);

        if (channelMap.isEmpty())
        {
            sessionMap.remove(sessionId);
            if (sessionMap.isEmpty())
            {
                this.channelMap.remove(channel);
            }
        }

        return value;
    }

    public interface ConnectionHandler<D, T>
    {
        void accept(final D channel, final Integer sessionId, final Integer streamId, final T value);
    }

    @SuppressWarnings("unchecked")
    public void forEach(final ConnectionHandler connectionHandler)
    {
        channelMap.forEach(
            (channel, sessionMap) ->
                sessionMap.forEach(
                    (sessionId, channelMap) ->
                        channelMap.forEach(
                            (streamId, value) -> connectionHandler.accept(channel, sessionId, streamId, value))));
    }
}

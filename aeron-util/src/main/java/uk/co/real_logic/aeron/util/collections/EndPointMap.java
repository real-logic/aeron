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
package uk.co.real_logic.aeron.util.collections;

import java.util.HashMap;
import java.util.Map;

import static uk.co.real_logic.aeron.util.collections.CollectionUtil.getOrDefault;

/**
 * Map for storing information about channel endpoints. These are keyed
 * by a triple of destination/session/channel.
 */
public class EndPointMap<D, E>
{
    private final Map<D, Long2ObjectHashMap<Long2ObjectHashMap<E>>> endPointByDestinationMap = new HashMap<>();

    public E get(final D destination, final long sessionId, final long channelId)
    {
        final Long2ObjectHashMap<Long2ObjectHashMap<E>> sessionMap = endPointByDestinationMap.get(destination);
        if (sessionMap == null)
        {
            return null;
        }

        final Long2ObjectHashMap<E> endPointMap = sessionMap.get(sessionId);
        if (endPointMap == null)
        {
            return null;
        }

        return endPointMap.get(channelId);
    }

    public E put(final D destination, final long sessionId, final long channelId, final E value)
    {
        final Long2ObjectHashMap<Long2ObjectHashMap<E>> endPointMap
            = getOrDefault(endPointByDestinationMap, destination, ignore -> new Long2ObjectHashMap<>());
        final Long2ObjectHashMap<E> channelMap = endPointMap.getOrDefault(sessionId, Long2ObjectHashMap::new);
        return channelMap.put(channelId, value);
    }

    public E remove(final D destination, final long sessionId, final long channelId)
    {
        final Long2ObjectHashMap<Long2ObjectHashMap<E>> sessionMap = endPointByDestinationMap.get(destination);
        if (sessionMap == null)
        {
            return null;
        }

        final Long2ObjectHashMap<E> endPointMap = sessionMap.get(sessionId);
        if (endPointMap == null)
        {
            return null;
        }

        E value = endPointMap.remove(channelId);

        if (endPointMap.isEmpty())
        {
            sessionMap.remove(sessionId);
            if (sessionMap.isEmpty())
            {
                endPointByDestinationMap.remove(destination);
            }
        }

        return value;
    }

    public interface EndPointHandler<D, T>
    {
        void accept(final D destination, final Long sessionId, final Long channelId, final T value);
    }

    @SuppressWarnings("unchecked")
    public void forEach(final EndPointHandler endPoint)
    {
        endPointByDestinationMap.forEach(
            (destination, sessionMap) ->
            {
                sessionMap.forEach(
                    (sessionId, channelMap) ->
                    {
                        channelMap.forEach(
                            (channelId, value) -> endPoint.accept(destination, sessionId, channelId, value));
                    });
            });
    }
}

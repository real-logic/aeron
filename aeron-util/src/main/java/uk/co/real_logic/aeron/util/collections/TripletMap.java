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

import static java.util.Map.Entry;

/**
 * Map-like data structure for mapping of sessionId/channelId/termId to a type.
 *
 * @param <V> type of object stored for the triplet.
 */
public class TripletMap<V>
{
    private final Long2ObjectHashMap<Long2ObjectHashMap<Long2ObjectHashMap<V>>> map = new Long2ObjectHashMap<>();

    public V get(final long sessionId, final long channelId, final long termId)
    {
        final Long2ObjectHashMap<Long2ObjectHashMap<V>> channelMap = map.get(sessionId);
        if (null == channelMap)
        {
            return null;
        }

        final Long2ObjectHashMap<V> termMap = channelMap.get(channelId);
        if (null == termMap)
        {
            return null;
        }

        return termMap.get(termId);
    }

    public V put(final long sessionId, final long channelId, final long termId, final V value)
    {
        Long2ObjectHashMap<Long2ObjectHashMap<V>> channelMap = map.get(sessionId);
        if (null == channelMap)
        {
            channelMap = new Long2ObjectHashMap<>();
            map.put(sessionId, channelMap);
        }

        Long2ObjectHashMap<V> termMap = channelMap.get(channelId);
        if (null == termMap)
        {
            termMap = new Long2ObjectHashMap<>();
            channelMap.put(channelId, termMap);
        }

        return termMap.put(termId, value);
    }

    public V remove(final long sessionId, final long channelId, final long termId)
    {
        final Long2ObjectHashMap<Long2ObjectHashMap<V>> channelMap = map.get(sessionId);
        if (null == channelMap)
        {
            return null;
        }

        final Long2ObjectHashMap<V> termMap = channelMap.get(channelId);
        if (null == termMap)
        {
            return null;
        }

        final V value = termMap.remove(termId);

        if (termMap.size() == 0)
        {
            channelMap.remove(channelId);
        }

        if (channelMap.size() == 0)
        {
            map.remove(sessionId);
        }

        return value;
    }

    public void forEach(TripletHandler<V> handler)
    {
        for (Entry<Long, Long2ObjectHashMap<Long2ObjectHashMap<V>>> sessionEntry :map.entrySet())
        {
            long sessionId = sessionEntry.getKey();
            for (Entry<Long, Long2ObjectHashMap<V>> channelEntry : sessionEntry.getValue().entrySet())
            {
                long channelId = channelEntry.getKey();
                for (Entry<Long, V> termEntry : channelEntry.getValue().entrySet())
                {
                    long termId = termEntry.getKey();
                    handler.handle(sessionId, channelId, termId, termEntry.getValue());
                }
            }
        }
    }

}

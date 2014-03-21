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

/**
 * Map-like data structure for mapping of sessionId/channelId/termId to a type.
 *
 * @param <V> type of object stored for the triplet.
 */
public class TripletMap<V>
{
    final Long2ObjectOpenAddressingHashMap<Long2ObjectOpenAddressingHashMap<Long2ObjectOpenAddressingHashMap<V>>> map =
        new Long2ObjectOpenAddressingHashMap<>();

    public V get(final long sessionId, final long channelId, final long termId)
    {
        final Long2ObjectOpenAddressingHashMap<Long2ObjectOpenAddressingHashMap<V>> channelMap = map.get(sessionId);
        if (null == channelMap)
        {
            return null;
        }

        final Long2ObjectOpenAddressingHashMap<V> termMap = channelMap.get(channelId);
        if (null == termMap)
        {
            return null;
        }

        return termMap.get(termId);
    }

    public V put(final long sessionId, final long channelId, final long termId, final V value)
    {
        Long2ObjectOpenAddressingHashMap<Long2ObjectOpenAddressingHashMap<V>> channelMap = map.get(sessionId);
        if (null == channelMap)
        {
            channelMap = new Long2ObjectOpenAddressingHashMap<>();
            map.put(sessionId, channelMap);
        }

        Long2ObjectOpenAddressingHashMap<V> termMap = channelMap.get(channelId);
        if (null == termMap)
        {
            termMap = new Long2ObjectOpenAddressingHashMap<>();
            channelMap.put(channelId, termMap);
        }

        return termMap.put(termId, value);
    }
}

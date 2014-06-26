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
package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.SubscriberChannel;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;

import java.util.HashMap;
import java.util.Map;

import static uk.co.real_logic.aeron.util.collections.CollectionUtil.getOrDefault;

public class SubscriberMap
{
    private final Map<String, Long2ObjectHashMap<SubscriberChannel>> map;

    public SubscriberMap()
    {
        map = new HashMap<>();
    }

    public SubscriberChannel get(final String destination, final long channelId)
    {
        final Long2ObjectHashMap<SubscriberChannel> channelMap = map.get(destination);
        if (channelMap == null)
        {
            return null;
        }

        return channelMap.get(channelId);
    }

    public void put(final String destination, final long channelId, final SubscriberChannel value)
    {
        getOrDefault(map, destination, (ignore) -> new Long2ObjectHashMap<>()).put(channelId, value);
    }

    public SubscriberChannel remove(final String destination, final long channelId)
    {
        final Long2ObjectHashMap<SubscriberChannel> channelMap = map.get(destination);
        if (channelMap == null)
        {
            return null;
        }

        SubscriberChannel value = channelMap.remove(channelId);

        if (channelMap.isEmpty())
        {
            channelMap.remove(channelId);
            if (channelMap.isEmpty())
            {
                map.remove(destination);
            }
        }

        return value;
    }
}

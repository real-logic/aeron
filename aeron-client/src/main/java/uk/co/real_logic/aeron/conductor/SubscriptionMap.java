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

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.collections.Int2ObjectHashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static uk.co.real_logic.aeron.common.collections.CollectionUtil.getOrDefault;

public class SubscriptionMap
{
    private static final Function<String, Int2ObjectHashMap<Subscription>> SUPPLIER = (ignore) -> new Int2ObjectHashMap<>();

    private final Map<String, Int2ObjectHashMap<Subscription>> subscriptionByChannelMap = new HashMap<>();

    public Subscription get(final String channel, final int streamId)
    {
        final Int2ObjectHashMap<Subscription> channelMap = subscriptionByChannelMap.get(channel);
        if (channelMap == null)
        {
            return null;
        }

        return channelMap.get(streamId);
    }

    public void put(final String channel, final int streamId, final Subscription value)
    {
        getOrDefault(subscriptionByChannelMap, channel, SUPPLIER).put(streamId, value);
    }

    public Subscription remove(final String channel, final int streamId)
    {
        final Int2ObjectHashMap<Subscription> streamMap = subscriptionByChannelMap.get(channel);
        if (streamMap == null)
        {
            return null;
        }

        final Subscription value = streamMap.remove(streamId);

        if (streamMap.isEmpty())
        {
            streamMap.remove(streamId);
            if (streamMap.isEmpty())
            {
                subscriptionByChannelMap.remove(channel);
            }
        }

        return value;
    }
}

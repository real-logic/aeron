/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron;

import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Int2ObjectHashMap;

import java.util.ArrayList;
import java.util.function.Consumer;

class ActiveSubscriptions
{
    private final Int2ObjectHashMap<ArrayList<Subscription>> subscriptionsByStreamIdMap = new Int2ObjectHashMap<>();

    public void forEach(final int streamId, final Consumer<Subscription> consumer)
    {
        final ArrayList<Subscription> subscriptions = subscriptionsByStreamIdMap.get(streamId);
        if (null != subscriptions)
        {
            subscriptions.forEach(consumer);
        }
    }

    public void add(final Subscription subscription)
    {
        ArrayList<Subscription> subscriptions = subscriptionsByStreamIdMap.get(subscription.streamId());
        if (null == subscriptions)
        {
            subscriptions = new ArrayList<>();
            subscriptionsByStreamIdMap.put(subscription.streamId(), subscriptions);
        }

        subscriptions.add(subscription);
    }

    public void remove(final Subscription subscription)
    {
        final int streamId = subscription.streamId();
        final ArrayList<Subscription> subscriptions = subscriptionsByStreamIdMap.get(streamId);

        if (null != subscriptions && remove(subscriptions, subscription) && subscriptions.isEmpty())
        {
            subscriptionsByStreamIdMap.remove(streamId);
        }
    }

    public void close()
    {
        for (final ArrayList<Subscription> subscriptions : subscriptionsByStreamIdMap.values())
        {
            for (int i = 0, size = subscriptions.size(); i < size; i++)
            {
                subscriptions.get(i).forceClose();
            }
        }

        subscriptionsByStreamIdMap.clear();
    }

    private static boolean remove(final ArrayList<Subscription> subscriptions, final Subscription subscription)
    {
        for (int i = 0, size = subscriptions.size(); i < size; i++)
        {
            if (subscription == subscriptions.get(i))
            {
                ArrayListUtil.fastUnorderedRemove(subscriptions, i, size - 1);
                return true;
            }
        }

        return false;
    }
}

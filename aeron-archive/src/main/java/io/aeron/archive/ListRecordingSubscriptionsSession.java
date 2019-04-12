/*
 * Copyright 2019 Real Logic Ltd.
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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.Subscription;
import org.agrona.collections.Object2ObjectHashMap;

class ListRecordingSubscriptionsSession implements Session
{
    private final long correlationId;
    private final int subscriptionCount;
    private int pseudoIndex;
    private int sent;
    private final int streamId;
    private boolean applyStreamId;
    private boolean isDone = false;
    private final String channelFragment;
    private final Object2ObjectHashMap<String, Subscription> subscriptionByKeyMap;
    private final ControlSession controlSession;
    private final ControlResponseProxy proxy;

    ListRecordingSubscriptionsSession(
        final Object2ObjectHashMap<String, Subscription> subscriptionByKeyMap,
        final int pseudoIndex,
        final int subscriptionCount,
        final int streamId,
        final boolean applyStreamId,
        final String channelFragment,
        final long correlationId,
        final ControlSession controlSession,
        final ControlResponseProxy proxy)
    {
        this.subscriptionByKeyMap = subscriptionByKeyMap;
        this.pseudoIndex = pseudoIndex;
        this.subscriptionCount = subscriptionCount;
        this.streamId = streamId;
        this.applyStreamId = applyStreamId;
        this.channelFragment = channelFragment;
        this.correlationId = correlationId;
        this.controlSession = controlSession;
        this.proxy = proxy;
    }

    public void close()
    {
        controlSession.activeListing(null);
    }

    public void abort()
    {
        isDone = true;
    }

    public boolean isDone()
    {
        return isDone;
    }

    public long sessionId()
    {
        return Aeron.NULL_VALUE;
    }

    public int doWork()
    {
        int workCount = 0;
        int index = 0;
        final int size = subscriptionByKeyMap.size();

        for (final Subscription subscription : subscriptionByKeyMap.values())
        {
            if (index++ >= pseudoIndex)
            {
                if (!(applyStreamId && subscription.streamId() != streamId) &&
                    subscription.channel().contains(channelFragment))
                {
                    if (!controlSession.sendSubscriptionDescriptor(correlationId, subscription, proxy))
                    {
                        break;
                    }

                    workCount += 1;

                    if (++sent >= subscriptionCount)
                    {
                        isDone = true;
                        break;
                    }
                }

                pseudoIndex = index - 1;
            }
        }

        if (!isDone && index >= size)
        {
            controlSession.sendSubscriptionUnknown(correlationId, proxy);
            isDone = true;
            workCount += 1;
        }

        return workCount;
    }
}

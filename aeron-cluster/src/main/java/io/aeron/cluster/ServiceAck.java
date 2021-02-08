/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.cluster.client.ClusterException;

import java.util.ArrayDeque;

/**
 * State holder for ACKs from each of the services.
 */
final class ServiceAck
{
    private final long ackId;
    private final long logPosition;
    private final long relevantId;

    ServiceAck(final long ackId, final long logPosition, final long relevantId)
    {
        this.logPosition = logPosition;
        this.ackId = ackId;
        this.relevantId = relevantId;
    }

    long ackId()
    {
        return ackId;
    }

    long logPosition()
    {
        return logPosition;
    }

    long relevantId()
    {
        return relevantId;
    }

    static boolean hasReached(final long logPosition, final long ackId, final ArrayDeque<ServiceAck>[] queues)
    {
        for (final ArrayDeque<ServiceAck> serviceAckQueue : queues)
        {
            final ServiceAck serviceAck = serviceAckQueue.peek();

            if (null == serviceAck)
            {
                return false;
            }

            if (serviceAck.ackId != ackId)
            {
                throw new ClusterException(ackId + " ack out of sequence " + serviceAck);
            }

            if (serviceAck.logPosition != logPosition)
            {
                throw new ClusterException(logPosition + " log position out of sequence " + serviceAck);
            }
        }

        return true;
    }

    static void removeHead(final ArrayDeque<ServiceAck>[] queues)
    {
        for (final ArrayDeque<ServiceAck> queue : queues)
        {
            queue.pollFirst();
        }
    }

    static ArrayDeque<ServiceAck>[] newArray(final int serviceCount)
    {
        @SuppressWarnings("unchecked") final ArrayDeque<ServiceAck>[] queues = new ArrayDeque[serviceCount];

        for (int i = 0; i < serviceCount; i++)
        {
            queues[i] = new ArrayDeque<>();
        }

        return queues;
    }

    public String toString()
    {
        return "ServiceAck{" +
            "ackId=" + ackId +
            ", logPosition=" + logPosition +
            ", relevantId=" + relevantId +
            '}';
    }
}

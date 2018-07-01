/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.cluster;

import static io.aeron.Aeron.NULL_VALUE;

/**
 * State holder for ACKs from each of the services.
 */
class ServiceAck
{
    private long logPosition = NULL_VALUE;
    private long ackId = NULL_VALUE;
    private long relevantId = NULL_VALUE;

    long logPosition()
    {
        return logPosition;
    }

    ServiceAck logPosition(final long logPosition)
    {
        this.logPosition = logPosition;
        return this;
    }

    long ackId()
    {
        return ackId;
    }

    ServiceAck ackId(final long ackId)
    {
        this.ackId = ackId;
        return this;
    }

    long relevantId()
    {
        return relevantId;
    }

    ServiceAck relevantId(final long relevantId)
    {
        this.relevantId = relevantId;
        return this;
    }

    static boolean hasReachedPosition(final long position, final long ackId, final ServiceAck[] serviceAcks)
    {
        for (final ServiceAck serviceAck : serviceAcks)
        {
            if (serviceAck.logPosition() < position || serviceAck.ackId() < ackId)
            {
                return false;
            }
        }

        return true;
    }

    static ServiceAck[] newArray(final int serviceCount)
    {
        final ServiceAck[] states = new ServiceAck[serviceCount];
        for (int i = 0; i < serviceCount; i++)
        {
            states[i] = new ServiceAck();
        }

        return states;
    }
}

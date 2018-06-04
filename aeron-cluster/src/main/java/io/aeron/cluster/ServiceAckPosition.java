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

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.service.ClusteredService.NULL_SERVICE_ID;

/**
 * State holder for each of the services.
 */
class ServiceAckPosition
{
    private long logPosition = NULL_POSITION;
    private long relevantId = NULL_SERVICE_ID;

    long logPosition()
    {
        return logPosition;
    }

    ServiceAckPosition logPosition(final long logPosition)
    {
        this.logPosition = logPosition;
        return this;
    }

    long relevantId()
    {
        return relevantId;
    }

    ServiceAckPosition relevantId(final long relevantId)
    {
        this.relevantId = relevantId;
        return this;
    }

    static void resetToNull(final ServiceAckPosition[] serviceAckPositions)
    {
        for (final ServiceAckPosition serviceAckPosition : serviceAckPositions)
        {
            serviceAckPosition.logPosition(NULL_POSITION).relevantId(NULL_SERVICE_ID);
        }
    }

    static boolean hasReachedThreshold(final long position, final ServiceAckPosition[] serviceAckPositions)
    {
        for (final ServiceAckPosition serviceAckPosition : serviceAckPositions)
        {
            if (serviceAckPosition.logPosition() < position)
            {
                return false;
            }
        }

        return true;
    }

    static ServiceAckPosition[] newArray(final int serviceCount)
    {
        final ServiceAckPosition[] states = new ServiceAckPosition[serviceCount];
        for (int i = 0; i < serviceCount; i++)
        {
            states[i] = new ServiceAckPosition();
        }

        return states;
    }
}

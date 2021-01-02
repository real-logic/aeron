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
package io.aeron.agent;

import net.bytebuddy.asm.Advice;

import java.net.InetSocketAddress;

import static io.aeron.agent.DriverEventCode.NAME_RESOLUTION_NEIGHBOR_ADDED;
import static io.aeron.agent.DriverEventCode.NAME_RESOLUTION_NEIGHBOR_REMOVED;
import static io.aeron.agent.DriverEventLogger.LOGGER;

/**
 * Diverse driver-related interceptors.
 */
class DriverInterceptor
{
    static class UntetheredSubscriptionStateChange
    {
        @Advice.OnMethodEnter
        static <E extends Enum<E>> void stateChange(
            final E oldState, final E newState, final long subscriptionId, final int streamId, final int sessionId)
        {
            LOGGER.logUntetheredSubscriptionStateChange(oldState, newState, subscriptionId, streamId, sessionId);
        }
    }

    static class Neighbour
    {
        @Advice.OnMethodEnter
        static void neighborAdded(final long nowMs, final InetSocketAddress address)
        {
            LOGGER.logAddress(NAME_RESOLUTION_NEIGHBOR_ADDED, address);
        }

        @Advice.OnMethodEnter
        static void neighborRemoved(final long nowMs, final InetSocketAddress address)
        {
            LOGGER.logAddress(NAME_RESOLUTION_NEIGHBOR_REMOVED, address);
        }
    }
}

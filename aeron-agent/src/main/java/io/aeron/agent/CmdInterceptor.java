/*
 * Copyright 2014-2020 Real Logic Limited.
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
import org.agrona.DirectBuffer;

import java.util.EnumMap;
import java.util.Map;

import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverEventLogger.LOGGER;
import static io.aeron.command.ControlProtocolEvents.*;
import static java.util.Collections.unmodifiableMap;

/**
 * Intercepts calls for the command protocol from clients to the driver for logging.
 */
final class CmdInterceptor
{
    static final Map<DriverEventCode, Class<?>> EVENT_TO_ADVICE_MAPPING;

    static
    {
        final Map<DriverEventCode, Class<?>> map = new EnumMap<>(DriverEventCode.class);
        map.put(CMD_IN_ADD_PUBLICATION, AddPublication.class);
        map.put(CMD_IN_ADD_EXCLUSIVE_PUBLICATION, AddExclusivePublication.class);
        map.put(CMD_IN_ADD_SUBSCRIPTION, AddSubcription.class);
        map.put(CMD_IN_REMOVE_PUBLICATION, RemovePublication.class);
        map.put(CMD_IN_REMOVE_SUBSCRIPTION, RemoveSubscription.class);
        map.put(CMD_IN_KEEPALIVE_CLIENT, ClientKeepAlive.class);
        map.put(CMD_IN_ADD_DESTINATION, AddDestination.class);
        map.put(CMD_IN_REMOVE_DESTINATION, RemoveDestination.class);
        map.put(CMD_OUT_AVAILABLE_IMAGE, OnAvailableImage.class);
        map.put(CMD_OUT_ERROR, OnError.class);
        map.put(CMD_OUT_ON_OPERATION_SUCCESS, OnOperationSuccess.class);
        map.put(CMD_OUT_PUBLICATION_READY, OnPublicationReady.class);
        map.put(CMD_OUT_ON_UNAVAILABLE_IMAGE, OnUnavailableImage.class);
        map.put(CMD_OUT_EXCLUSIVE_PUBLICATION_READY, OnExclusivePublicationReady.class);
        map.put(CMD_OUT_SUBSCRIPTION_READY, OnSubscriptionReady.class);
        map.put(CMD_OUT_COUNTER_READY, OnCounterReady.class);
        map.put(CMD_OUT_ON_UNAVAILABLE_COUNTER, OnUnavailableCounter.class);
        map.put(CMD_IN_ADD_COUNTER, AddCounter.class);
        map.put(CMD_IN_REMOVE_COUNTER, RemoveCounter.class);
        map.put(CMD_IN_CLIENT_CLOSE, ClientClose.class);
        map.put(CMD_IN_ADD_RCV_DESTINATION, AddRcvDestination.class);
        map.put(CMD_IN_REMOVE_RCV_DESTINATION, RemoveRcvDestination.class);
        map.put(CMD_OUT_ON_CLIENT_TIMEOUT, OnClientTimeout.class);
        map.put(CMD_IN_TERMINATE_DRIVER, TerminateDriver.class);
        EVENT_TO_ADVICE_MAPPING = unmodifiableMap(map);
    }

    static final class AddPublication
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ADD_PUBLICATION == msgTypeId)
            {
                LOGGER.log(CMD_IN_ADD_PUBLICATION, buffer, index, length);
            }
        }
    }

    static final class AddExclusivePublication
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ADD_EXCLUSIVE_PUBLICATION == msgTypeId)
            {
                LOGGER.log(CMD_IN_ADD_EXCLUSIVE_PUBLICATION, buffer, index, length);
            }
        }
    }

    static final class AddSubcription
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ADD_SUBSCRIPTION == msgTypeId)
            {
                LOGGER.log(CMD_IN_ADD_SUBSCRIPTION, buffer, index, length);
            }
        }
    }

    static final class RemovePublication
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (REMOVE_PUBLICATION == msgTypeId)
            {
                LOGGER.log(CMD_IN_REMOVE_PUBLICATION, buffer, index, length);
            }
        }
    }

    static final class RemoveSubscription
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (REMOVE_SUBSCRIPTION == msgTypeId)
            {
                LOGGER.log(CMD_IN_REMOVE_SUBSCRIPTION, buffer, index, length);
            }
        }
    }

    static final class ClientKeepAlive
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (CLIENT_KEEPALIVE == msgTypeId)
            {
                LOGGER.log(CMD_IN_KEEPALIVE_CLIENT, buffer, index, length);
            }
        }
    }

    static final class AddDestination
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ADD_DESTINATION == msgTypeId)
            {
                LOGGER.log(CMD_IN_ADD_DESTINATION, buffer, index, length);
            }
        }
    }

    static final class RemoveDestination
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (REMOVE_DESTINATION == msgTypeId)
            {
                LOGGER.log(CMD_IN_REMOVE_DESTINATION, buffer, index, length);
            }
        }
    }

    static final class OnAvailableImage
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ON_AVAILABLE_IMAGE == msgTypeId)
            {
                LOGGER.log(CMD_OUT_AVAILABLE_IMAGE, buffer, index, length);
            }
        }
    }

    static final class OnError
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ON_ERROR == msgTypeId)
            {
                LOGGER.log(CMD_OUT_ERROR, buffer, index, length);
            }
        }
    }

    static final class OnOperationSuccess
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ON_OPERATION_SUCCESS == msgTypeId)
            {
                LOGGER.log(CMD_OUT_ON_OPERATION_SUCCESS, buffer, index, length);
            }
        }
    }

    static final class OnPublicationReady
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ON_PUBLICATION_READY == msgTypeId)
            {
                LOGGER.log(CMD_OUT_PUBLICATION_READY, buffer, index, length);
            }
        }
    }

    static final class OnUnavailableImage
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ON_UNAVAILABLE_IMAGE == msgTypeId)
            {
                LOGGER.log(CMD_OUT_ON_UNAVAILABLE_IMAGE, buffer, index, length);
            }
        }
    }

    static final class OnExclusivePublicationReady
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ON_EXCLUSIVE_PUBLICATION_READY == msgTypeId)
            {
                LOGGER.log(CMD_OUT_EXCLUSIVE_PUBLICATION_READY, buffer, index, length);
            }
        }
    }

    static final class OnSubscriptionReady
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ON_SUBSCRIPTION_READY == msgTypeId)
            {
                LOGGER.log(CMD_OUT_SUBSCRIPTION_READY, buffer, index, length);
            }
        }
    }

    static final class OnCounterReady
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ON_COUNTER_READY == msgTypeId)
            {
                LOGGER.log(CMD_OUT_COUNTER_READY, buffer, index, length);
            }
        }
    }

    static final class OnUnavailableCounter
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ON_UNAVAILABLE_COUNTER == msgTypeId)
            {
                LOGGER.log(CMD_OUT_ON_UNAVAILABLE_COUNTER, buffer, index, length);
            }
        }
    }

    static final class AddCounter
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ADD_COUNTER == msgTypeId)
            {
                LOGGER.log(CMD_IN_ADD_COUNTER, buffer, index, length);
            }
        }
    }

    static final class RemoveCounter
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (REMOVE_COUNTER == msgTypeId)
            {
                LOGGER.log(CMD_IN_REMOVE_COUNTER, buffer, index, length);
            }
        }
    }

    static final class ClientClose
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (CLIENT_CLOSE == msgTypeId)
            {
                LOGGER.log(CMD_IN_CLIENT_CLOSE, buffer, index, length);
            }
        }
    }

    static final class AddRcvDestination
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ADD_RCV_DESTINATION == msgTypeId)
            {
                LOGGER.log(CMD_IN_ADD_RCV_DESTINATION, buffer, index, length);
            }
        }
    }

    static final class RemoveRcvDestination
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (REMOVE_RCV_DESTINATION == msgTypeId)
            {
                LOGGER.log(CMD_IN_REMOVE_RCV_DESTINATION, buffer, index, length);
            }
        }
    }

    static final class OnClientTimeout
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (ON_CLIENT_TIMEOUT == msgTypeId)
            {
                LOGGER.log(CMD_OUT_ON_CLIENT_TIMEOUT, buffer, index, length);
            }
        }
    }

    static final class TerminateDriver
    {
        @Advice.OnMethodEnter
        static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
        {
            if (TERMINATE_DRIVER == msgTypeId)
            {
                LOGGER.log(CMD_IN_TERMINATE_DRIVER, buffer, index, length);
            }
        }
    }

    private CmdInterceptor()
    {
    }
}

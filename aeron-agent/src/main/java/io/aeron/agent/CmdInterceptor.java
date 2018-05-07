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
package io.aeron.agent;

import net.bytebuddy.asm.Advice;
import org.agrona.DirectBuffer;

import static io.aeron.agent.EventCode.*;
import static io.aeron.agent.EventLogger.LOGGER;
import static io.aeron.command.ControlProtocolEvents.*;

/**
 * Intercepts calls for the command protocol from clients to the driver for logging.
 */
public class CmdInterceptor
{
    @Advice.OnMethodEnter
    public static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
    {
        switch (msgTypeId)
        {
            case ADD_PUBLICATION:
                LOGGER.log(CMD_IN_ADD_PUBLICATION, buffer, index, length);
                break;

            case REMOVE_PUBLICATION:
                LOGGER.log(CMD_IN_REMOVE_PUBLICATION, buffer, index, length);
                break;

            case ADD_EXCLUSIVE_PUBLICATION:
                LOGGER.log(CMD_IN_ADD_EXCLUSIVE_PUBLICATION, buffer, index, length);
                break;

            case ADD_SUBSCRIPTION:
                LOGGER.log(CMD_IN_ADD_SUBSCRIPTION, buffer, index, length);
                break;

            case REMOVE_SUBSCRIPTION:
                LOGGER.log(CMD_IN_REMOVE_SUBSCRIPTION, buffer, index, length);
                break;

            case CLIENT_KEEPALIVE:
                LOGGER.log(CMD_IN_KEEPALIVE_CLIENT, buffer, index, length);
                break;

            case ADD_DESTINATION:
                LOGGER.log(CMD_IN_ADD_DESTINATION, buffer, index, length);
                break;

            case REMOVE_DESTINATION:
                LOGGER.log(CMD_IN_REMOVE_DESTINATION, buffer, index, length);
                break;

            case ON_AVAILABLE_IMAGE:
                LOGGER.log(CMD_OUT_AVAILABLE_IMAGE, buffer, index, length);
                break;

            case ON_ERROR:
                LOGGER.log(CMD_OUT_ERROR, buffer, index, length);
                break;

            case ON_OPERATION_SUCCESS:
                LOGGER.log(CMD_OUT_ON_OPERATION_SUCCESS, buffer, index, length);
                break;

            case ON_PUBLICATION_READY:
                LOGGER.log(CMD_OUT_PUBLICATION_READY, buffer, index, length);
                break;

            case ON_UNAVAILABLE_IMAGE:
                LOGGER.log(CMD_OUT_ON_UNAVAILABLE_IMAGE, buffer, index, length);
                break;

            case ON_EXCLUSIVE_PUBLICATION_READY:
                LOGGER.log(CMD_OUT_EXCLUSIVE_PUBLICATION_READY, buffer, index, length);
                break;

            case ON_SUBSCRIPTION_READY:
                LOGGER.log(CMD_OUT_SUBSCRIPTION_READY, buffer, index, length);
                break;

            case ON_COUNTER_READY:
                LOGGER.log(CMD_OUT_COUNTER_READY, buffer, index, length);
                break;

            case ON_UNAVAILABLE_COUNTER:
                LOGGER.log(CMD_OUT_ON_UNAVAILABLE_COUNTER, buffer, index, length);
                break;

            case ADD_COUNTER:
                LOGGER.log(CMD_IN_ADD_COUNTER, buffer, index, length);
                break;

            case REMOVE_COUNTER:
                LOGGER.log(CMD_IN_REMOVE_COUNTER, buffer, index, length);
                break;

            case CLIENT_CLOSE:
                LOGGER.log(CMD_IN_CLIENT_CLOSE, buffer, index, length);
                break;

            case ADD_RCV_DESTINATION:
                LOGGER.log(CMD_IN_ADD_RCV_DESTINATION, buffer, index, length);
                break;

            case REMOVE_RCV_DESTINATION:
                LOGGER.log(CMD_IN_REMOVE_RCV_DESTINATION, buffer, index, length);
                break;
        }
    }
}

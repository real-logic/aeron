/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

import org.agrona.DirectBuffer;

import static io.aeron.command.ControlProtocolEvents.*;

public class CmdInterceptor
{
    public static void logCmd(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
    {
        switch (msgTypeId)
        {
            case ADD_PUBLICATION:
                EventLogger.LOGGER.log(EventCode.CMD_IN_ADD_PUBLICATION, buffer, index, length);
                break;

            case REMOVE_PUBLICATION:
                EventLogger.LOGGER.log(EventCode.CMD_IN_REMOVE_PUBLICATION, buffer, index, length);
                break;

            case ADD_SUBSCRIPTION:
                EventLogger.LOGGER.log(EventCode.CMD_IN_ADD_SUBSCRIPTION, buffer, index, length);
                break;

            case REMOVE_SUBSCRIPTION:
                EventLogger.LOGGER.log(EventCode.CMD_IN_REMOVE_SUBSCRIPTION, buffer, index, length);
                break;

            case CLIENT_KEEPALIVE:
                EventLogger.LOGGER.log(EventCode.CMD_IN_KEEPALIVE_CLIENT, buffer, index, length);
                break;

            case ON_AVAILABLE_IMAGE:
                EventLogger.LOGGER.log(EventCode.CMD_OUT_AVAILABLE_IMAGE, buffer, index, length);
                break;

            case ON_ERROR:
                // TODO: add event code and dissector
                break;

            case ON_OPERATION_SUCCESS:
                EventLogger.LOGGER.log(EventCode.CMD_OUT_ON_OPERATION_SUCCESS, buffer, index, length);
                break;

            case ON_PUBLICATION_READY:
                EventLogger.LOGGER.log(EventCode.CMD_OUT_PUBLICATION_READY, buffer, index, length);
                break;

            case ON_UNAVAILABLE_IMAGE:
                EventLogger.LOGGER.log(EventCode.CMD_OUT_ON_UNAVAILABLE_IMAGE, buffer, index, length);
                break;
        }
    }
}

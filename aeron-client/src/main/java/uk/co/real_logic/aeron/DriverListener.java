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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.common.ErrorCode;
import uk.co.real_logic.aeron.common.command.ConnectionMessageFlyweight;
import uk.co.real_logic.aeron.common.command.ConnectionReadyFlyweight;
import uk.co.real_logic.aeron.common.command.ReadyFlyweight;

/**
 * Callback interface for receiving messages from the driver.
 */
interface DriverListener
{
    void onNewPublication(
            String channel,
            int streamId,
            int sessionId,
            int termId,
            int positionIndicatorId,
            ReadyFlyweight message,
            long correlationId, final int mtuLength);

    void onNewConnection(
            String channel,
            int streamId,
            int sessionId,
            int termId,
            long initialPosition,
            ConnectionReadyFlyweight message,
            long correlationId);

    void onInactiveConnection(
            String channel,
            int streamId,
            int sessionId,
            ConnectionMessageFlyweight message,
            long correlationId);

    void onError(ErrorCode errorCode, String message);

    void operationSucceeded();
}

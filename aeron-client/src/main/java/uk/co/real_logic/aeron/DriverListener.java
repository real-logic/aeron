/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import uk.co.real_logic.agrona.collections.Long2LongHashMap;

/**
 * Callback interface for receiving messages from the driver.
 */
interface DriverListener
{
    void onNewPublication(
        String channel,
        int streamId,
        int sessionId,
        int publicationLimitId,
        String logFileName,
        long correlationId);

    void onAvailableImage(
        int streamId,
        int sessionId,
        Long2LongHashMap subscriberPositionMap,
        String logFileName,
        String sourceIdentity,
        long correlationId);

    void onUnavailableImage(int streamId, long correlationId);

    void onError(ErrorCode errorCode, String message, long correlationId);
}

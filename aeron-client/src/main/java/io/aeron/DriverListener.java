/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron;

/**
 * Callback interface for dispatching command responses from the driver on the control protocol.
 */
interface DriverListener
{
    void onError(long correlationId, ErrorCode errorCode, String message);

    void onAvailableImage(
                             long correlationId,
                             int streamId,
                             int sessionId,
                             long subscriberPositionRegistrationId, int subscriberPositionCounterId,
                             String logFileName,
                             String sourceIdentity);

    void onNewPublication(
        long correlationId,
        long registrationId,
        int streamId,
        int sessionId,
        int publicationLimitId,
        String channel,
        String logFileName);

    void onUnavailableImage(long correlationId, int streamId);

    void onNewExclusivePublication(
        long correlationId,
        long registrationId,
        int streamId,
        int sessionId,
        int publicationLimitId,
        String channel,
        String logFileName);
}

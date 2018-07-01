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
package io.aeron;

/**
 * Callback interface for dispatching driver events on the control protocol.
 */
interface DriverEventsListener
{
    void onError(long correlationId, int codeValue, ErrorCode errorCode, String message);

    void onAvailableImage(
        long correlationId,
        int streamId,
        int sessionId,
        long subscriptionRegistrationId,
        int subscriberPositionId,
        String logFileName,
        String sourceIdentity);

    void onNewPublication(
        long correlationId,
        long registrationId,
        int streamId,
        int sessionId,
        int publicationLimitId,
        int statusIndicatorId,
        String logFileName);

    void onNewSubscription(
        long correlationId,
        int statusIndicatorId);

    void onUnavailableImage(
        long correlationId,
        long subscriptionRegistrationId,
        int streamId);

    void onNewExclusivePublication(
        long correlationId,
        long registrationId,
        int streamId,
        int sessionId,
        int publicationLimitId,
        int statusIndicatorId,
        String logFileName);

    void onChannelEndpointError(
        int statusIndicatorId,
        String message);

    void onNewCounter(
        long correlationId,
        int counterId);

    void onAvailableCounter(
        long correlationId,
        int counterId);

    void onUnavailableCounter(
        long correlationId,
        int counterId);
}

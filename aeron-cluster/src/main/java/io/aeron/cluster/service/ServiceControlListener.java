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
package io.aeron.cluster.service;

import io.aeron.cluster.codecs.ClusterAction;

/**
 * Listens for events that can be bi-directional between the consensus module and services.
 * <p>
 * The relevant handlers can be implemented and others ignored with the default implementations.
 */
public interface ServiceControlListener
{
    /**
     * Request from a service to schedule a timer.
     *
     * @param correlationId that must be unique across services for the timer.
     * @param deadline      after which the timer will expire and then fire.
     */
    default void onScheduleTimer(long correlationId, long deadline)
    {
    }

    /**
     * Request from a service to cancel a previously scheduled timer.
     *
     * @param correlationId of the previously scheduled timer.
     */
    default void onCancelTimer(long correlationId)
    {
    }

    /**
     * Acknowledgement from a service that it has undertaken the a requested {@link ClusterAction}.
     *
     * @param logPosition      of the service after undertaking the action.
     * @param leadershipTermId within which the action has taken place.
     * @param serviceId        that has undertaken the action.
     * @param action           undertaken.
     */
    default void onServiceAck(long logPosition, long leadershipTermId, int serviceId, ClusterAction action)
    {
    }

    /**
     * Request that the services join to a log for replay or live stream.
     *
     * @param leadershipTermId for the log.
     * @param commitPositionId for counter that gives the bound for consumption of the log.
     * @param logSessionId     for the log to confirm subscription.
     * @param logStreamId      to subscribe to for the log.
     * @param logChannel       to subscribe to for the log.
     */
    default void onJoinLog(
        long leadershipTermId, int commitPositionId, int logSessionId, int logStreamId, String logChannel)
    {
    }

    /**
     * Request that a cluster session be closed.
     *
     * @param clusterSessionId of the session to be closed.
     */
    default void onServiceCloseSession(long clusterSessionId)
    {
    }
}

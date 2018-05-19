/*
 * Copyright 2018 Real Logic Ltd.
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

public interface ConsensusModuleListener
{
    /**
     * Request that the services join a newly available for log for replay or a live stream.
     *
     * @param leadershipTermId for the log.
     * @param commitPositionId for counter that gives the bound for consumption of the log.
     * @param logSessionId     for the log to confirm subscription.
     * @param logStreamId      to subscribe to for the log.
     * @param ackBeforeImage   or after Image.
     * @param logChannel       to subscribe to for the log.
     */
    void onJoinLog(
        long leadershipTermId,
        int commitPositionId,
        int logSessionId,
        int logStreamId,
        boolean ackBeforeImage,
        String logChannel);
}

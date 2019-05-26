/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.Election;
import io.aeron.cluster.service.Cluster;
import net.bytebuddy.asm.Advice;

import static io.aeron.agent.ClusterEventLogger.LOGGER;

/**
 * Intercepts calls in the cluster which relate to state changes.
 */
final class ClusterInterceptor
{
    static class ElectionStateChange
    {
        @Advice.OnMethodEnter
        static void state(final Election.State newState, final long nowMs, @Advice.This final Election election)
        {
            LOGGER.logElectionStateChange(newState, nowMs, election);
        }
    }

    static class NewLeadershipTerm
    {
        @Advice.OnMethodEnter
        static void onNewLeadershipTerm(
            final long logLeadershipTermId,
            final long logPosition,
            final long leadershipTermId,
            final long maxLogPosition,
            final int leaderMemberId,
            final int logSessionId)
        {
            LOGGER.logNewLeadershipTerm(
                logLeadershipTermId,
                logPosition,
                leadershipTermId,
                maxLogPosition,
                leaderMemberId,
                logSessionId);
        }
    }

    static class ConsensusModuleStateChange
    {
        @Advice.OnMethodEnter
        static void state(final ConsensusModule.State state)
        {
            LOGGER.logStateChange(state);
        }
    }

    static class ConsensusModuleRoleChange
    {
        @Advice.OnMethodEnter
        static void role(final Cluster.Role role)
        {
            LOGGER.logRoleChange(role);
        }
    }
}
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

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

class ClusterEventDissector
{
    static void electionStateChange(
        @SuppressWarnings("unused") final ClusterEventCode event,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int eventOffset = 0;

        final int memberId = buffer.getInt(offset + eventOffset);
        eventOffset += SIZE_OF_INT;

        final String stateChange = buffer.getStringAscii(offset + eventOffset);
        builder
            .append("CLUSTER: Election State ").append(stateChange)
            .append(", memberId=").append(memberId);
    }

    static void newLeadershipTerm(
        @SuppressWarnings("unused") final ClusterEventCode event,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int relativeOffset = offset;
        final long logLeadershipTermId = buffer.getLong(relativeOffset);
        relativeOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(relativeOffset);
        relativeOffset += SIZE_OF_LONG;
        final long leadershipTermId = buffer.getLong(relativeOffset);
        relativeOffset += SIZE_OF_LONG;
        final long maxLogPosition = buffer.getLong(relativeOffset);
        relativeOffset += SIZE_OF_LONG;
        final int leaderMemberId = buffer.getInt(relativeOffset);
        relativeOffset += BitUtil.SIZE_OF_INT;
        final int logSessionId = buffer.getInt(relativeOffset);

        builder.append("CLUSTER: New Leadership Term; logLeadershipTermId: ").append(logLeadershipTermId)
            .append(", logPosition: ").append(logPosition)
            .append(", leadershipTermId: ").append(leadershipTermId)
            .append(", maxLogPosition: ").append(maxLogPosition)
            .append(", leaderMemberId: ").append(leaderMemberId)
            .append(", logSessionId: ").append(logSessionId);
    }

    static void stateChange(
        @SuppressWarnings("unused") final ClusterEventCode event,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int eventOffset = 0;

        final int memberId = buffer.getInt(offset + eventOffset);
        eventOffset += SIZE_OF_INT;

        final String stateChange = buffer.getStringAscii(offset + eventOffset);
        builder
            .append("CLUSTER: ConsensusModule State ").append(stateChange)
            .append(", memberId=").append(memberId);
    }

    static void roleChange(
        @SuppressWarnings("unused") final ClusterEventCode event,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        builder.append("CLUSTER: ConsensusModule Role ").append(buffer.getStringAscii(offset));
    }
}
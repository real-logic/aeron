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

import org.agrona.MutableDirectBuffer;

final class ClusterEventDissector
{
    static void electionStateChange(
        final ClusterEventCode event, final MutableDirectBuffer buffer,
        final int offset, final StringBuilder builder)
    {
        final String stateName = buffer.getStringAscii(offset);
        final long timestampMs = buffer.getLong(offset + stateName.length() + Integer.BYTES);
        builder.append("CLUSTER:Election State -> ").append(stateName).append(' ').append(timestampMs);
    }

    static void newLeadershipTerm(
        final ClusterEventCode event, final MutableDirectBuffer buffer,
        final int offset, final StringBuilder builder)
    {
        int relativeOffset = offset;
        final long logLeadershipTermId = buffer.getLong(relativeOffset);
        relativeOffset += Long.BYTES;
        final long logPosition = buffer.getLong(relativeOffset);
        relativeOffset += Long.BYTES;
        final long leadershipTermId = buffer.getLong(relativeOffset);
        relativeOffset += Long.BYTES;
        final long maxLogPosition = buffer.getLong(relativeOffset);
        relativeOffset += Long.BYTES;
        final int leaderMemberId = buffer.getInt(relativeOffset);
        relativeOffset += Integer.BYTES;
        final int logSessionId = buffer.getInt(relativeOffset);

        builder.append("CLUSTER:New Leadership Term; logLeadershipTermId: ").append(logLeadershipTermId)
            .append(", logPosition: ").append(logPosition)
            .append(", leadershipTermId: ").append(leadershipTermId)
            .append(", maxLogPosition: ").append(maxLogPosition)
            .append(", leaderMemberId: ").append(leaderMemberId)
            .append(", logSessionId: ").append(logSessionId);
    }

    static void stateChange(
        final ClusterEventCode event, final MutableDirectBuffer buffer,
        final int offset, final StringBuilder builder)
    {
        builder.append("CLUSTER:ConsensusModule State -> ").append(buffer.getStringAscii(offset));
    }

    static void roleChange(
        final ClusterEventCode event, final MutableDirectBuffer buffer,
        final int offset, final StringBuilder builder)
    {
        builder.append("CLUSTER:Role -> ").append(buffer.getStringAscii(offset));
    }
}
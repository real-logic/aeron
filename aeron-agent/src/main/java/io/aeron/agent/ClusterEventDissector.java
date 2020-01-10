/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int eventOffset = 0;

        final int memberId = buffer.getInt(offset + eventOffset);
        eventOffset += SIZE_OF_INT;

        final String stateChange = buffer.getStringAscii(offset + eventOffset);
        builder
            .append("CLUSTER: ").append(eventCode.name())
            .append(", ").append(stateChange)
            .append(", memberId=").append(memberId);
    }

    static void newLeadershipTerm(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int relativeOffset = offset;
        final long logLeadershipTermId = buffer.getLong(relativeOffset);
        relativeOffset += SIZE_OF_LONG;
        final long leadershipTermId = buffer.getLong(relativeOffset);
        relativeOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(relativeOffset);
        relativeOffset += SIZE_OF_LONG;
        final long timestamp = buffer.getLong(relativeOffset);
        relativeOffset += SIZE_OF_LONG;
        final int leaderMemberId = buffer.getInt(relativeOffset);
        relativeOffset += BitUtil.SIZE_OF_INT;
        final int logSessionId = buffer.getInt(relativeOffset);

        builder.append("CLUSTER: ").append(eventCode.name())
            .append(", logLeadershipTermId=").append(logLeadershipTermId)
            .append(", leadershipTermId=").append(leadershipTermId)
            .append(", logPosition=").append(logPosition)
            .append(", timestamp=").append(timestamp)
            .append(", leaderMemberId=").append(leaderMemberId)
            .append(", logSessionId=").append(logSessionId);
    }

    static void stateChange(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int eventOffset = 0;

        final int memberId = buffer.getInt(offset + eventOffset);
        eventOffset += SIZE_OF_INT;

        builder.append("CLUSTER: ").append(eventCode.name()).append(", ");
        buffer.getStringAscii(offset + eventOffset, builder);
        builder.append(", memberId=").append(memberId);
    }

    static void roleChange(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int eventOffset = 0;

        final int memberId = buffer.getInt(offset + eventOffset);
        eventOffset += SIZE_OF_INT;

        builder.append("CLUSTER: ").append(eventCode.name()).append(", ");
        buffer.getStringAscii(offset + eventOffset, builder);
        builder.append(", memberId=").append(memberId);
    }
}

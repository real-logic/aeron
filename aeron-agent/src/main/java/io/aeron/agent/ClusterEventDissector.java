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
        builder.append("Election State -> ").append(stateName).append(' ').append(timestampMs);
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

        builder.append("New Leadership Term; logLeadershipTermId: ").append(logLeadershipTermId)
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
        builder.append("ConsensusModule State -> ").append(buffer.getStringAscii(offset));
    }

    static void roleChange(
        final ClusterEventCode event, final MutableDirectBuffer buffer,
        final int offset, final StringBuilder builder)
    {
        builder.append("Role -> ").append(buffer.getStringAscii(offset));
    }
}
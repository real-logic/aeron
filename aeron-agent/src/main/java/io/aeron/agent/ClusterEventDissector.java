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
        builder.append(stateName).append(' ').append(timestampMs);
    }
}
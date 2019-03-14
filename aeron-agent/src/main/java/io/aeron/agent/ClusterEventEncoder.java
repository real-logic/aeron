package io.aeron.agent;

import io.aeron.cluster.Election;
import org.agrona.MutableDirectBuffer;

final class ClusterEventEncoder
{
    static int encode(
        final MutableDirectBuffer encodedBuffer,
        final Election.State newState,
        final long nowMs)
    {
        final int timestampOffset = encodedBuffer.putStringAscii(0, newState.name());
        encodedBuffer.putLong(timestampOffset, nowMs);
        return timestampOffset + Long.BYTES;
    }
}

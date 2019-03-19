package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

final class ArchiveEventDissector
{
    static void connect(
        final ArchiveEventCode event, final MutableDirectBuffer buffer,
        final int offset, final StringBuilder builder)
    {
        int relativeOffset = offset;
        final long correlationId = buffer.getLong(relativeOffset);
        relativeOffset += Long.BYTES;
        final int streamId = buffer.getInt(relativeOffset);
        relativeOffset += Integer.BYTES;
        final int version = buffer.getInt(relativeOffset);
        relativeOffset += Integer.BYTES;
        final String channel = buffer.getStringAscii(relativeOffset);
        builder.append("ARCHIVE:CONNECT ")
            .append(channel).append(' ').append(streamId)
            .append(" [").append(correlationId).append("][")
            .append(version).append(']');
    }
}
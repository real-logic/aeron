package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

final class ArchiveEventEncoder
{
    static int encodeConnect(
        final MutableDirectBuffer encodedBuffer, final long correlationId,
        final int streamId, final int version, final String channel)
    {
        int offset = 0;
        encodedBuffer.putLong(offset, correlationId);
        offset += Long.BYTES;
        encodedBuffer.putInt(offset, streamId);
        offset += Integer.BYTES;
        encodedBuffer.putInt(offset, version);
        offset += Integer.BYTES;
        offset += encodedBuffer.putStringAscii(offset, channel);
        return offset;
    }
}

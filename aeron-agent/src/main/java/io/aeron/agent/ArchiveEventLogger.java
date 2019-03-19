package io.aeron.agent;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.nio.ByteBuffer;

public final class ArchiveEventLogger
{
    static final long ENABLED_EVENT_CODES = EventConfiguration.getEnabledArchiveEventCodes();
    public static final ArchiveEventLogger LOGGER = new ArchiveEventLogger(EventConfiguration.EVENT_RING_BUFFER);
    private static final ThreadLocal<MutableDirectBuffer> ENCODING_BUFFER = ThreadLocal.withInitial(
        () -> new UnsafeBuffer(ByteBuffer.allocateDirect(EventConfiguration.MAX_EVENT_LENGTH)));

    private final ManyToOneRingBuffer ringBuffer;

    private ArchiveEventLogger(final ManyToOneRingBuffer eventRingBuffer)
    {
        ringBuffer = eventRingBuffer;
    }

    public void logConnect(
        final long correlationId,
        final int streamId,
        final int version,
        final String channel)
    {
        if (ArchiveEventCode.isEnabled(ArchiveEventCode.CMD_IN_CONNECT, ENABLED_EVENT_CODES))
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = ArchiveEventEncoder.encodeConnect(
                encodedBuffer, correlationId, streamId, version, channel);

            ringBuffer.write(toEventCodeId(ArchiveEventCode.CMD_IN_CONNECT), encodedBuffer, 0, encodedLength);
        }
    }

    private static int toEventCodeId(final ArchiveEventCode code)
    {
        return ArchiveEventCode.EVENT_CODE_TYPE << 16 | (code.id() & 0xFFFF);
    }
}

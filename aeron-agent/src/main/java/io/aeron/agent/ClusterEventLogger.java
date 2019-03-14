package io.aeron.agent;

import io.aeron.cluster.Election;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.nio.ByteBuffer;

import static io.aeron.agent.ClusterEventCode.ELECTION_STATE_CHANGE;

public final class ClusterEventLogger
{
    static final long ENABLED_EVENT_CODES = EventConfiguration.getEnabledClusterEventCodes();
    public static final ClusterEventLogger LOGGER = new ClusterEventLogger(EventConfiguration.EVENT_RING_BUFFER);
    private static final ThreadLocal<MutableDirectBuffer> ENCODING_BUFFER = ThreadLocal.withInitial(
        () -> new UnsafeBuffer(ByteBuffer.allocateDirect(EventConfiguration.MAX_EVENT_LENGTH)));

    private final ManyToOneRingBuffer ringBuffer;

    private ClusterEventLogger(final ManyToOneRingBuffer eventRingBuffer)
    {
        ringBuffer = eventRingBuffer;
    }

    public void logElectionStateChange(final Election.State newState, final long nowMs)
    {
        if (ClusterEventCode.isEnabled(ELECTION_STATE_CHANGE, ENABLED_EVENT_CODES))
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = ClusterEventEncoder.encode(encodedBuffer, newState, nowMs);

            ringBuffer.write(toEventCodeId(ELECTION_STATE_CHANGE), encodedBuffer, 0, encodedLength);
        }
    }

    private static int toEventCodeId(final ClusterEventCode code)
    {
        return ClusterEventCode.EVENT_CODE_TYPE << 16 | (code.id() & 0xFFFF);
    }
}

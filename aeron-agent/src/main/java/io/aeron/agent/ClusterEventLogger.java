package io.aeron.agent;

import io.aeron.cluster.Election;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

final class ClusterEventLogger
{
    static final ClusterEventLogger LOGGER = new ClusterEventLogger(EventConfiguration.EVENT_RING_BUFFER);
    private final ManyToOneRingBuffer ringBuffer;

    private ClusterEventLogger(final ManyToOneRingBuffer eventRingBuffer)
    {
        ringBuffer = eventRingBuffer;
    }


    void logElectionStateChange(final Election.State newState, final long nowMs)
    {

    }
}

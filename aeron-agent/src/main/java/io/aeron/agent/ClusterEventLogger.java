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

import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.Election;
import io.aeron.cluster.service.Cluster;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static io.aeron.agent.ClusterEventCode.*;

/**
 * Event logger interface used by interceptors for recording cluster events into a {@link RingBuffer} for a
 * {@link ConsensusModule} events via a Java Agent.
 */
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

    public void logElectionStateChange(final Election.State oldState, final Election.State newState, final int memberId)
    {
        if (ClusterEventCode.isEnabled(ELECTION_STATE_CHANGE, ENABLED_EVENT_CODES))
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = ClusterEventEncoder.encodeElectionStateChange(
                encodedBuffer, oldState, newState, memberId);

            ringBuffer.write(toEventCodeId(ELECTION_STATE_CHANGE), encodedBuffer, 0, encodedLength);
        }
    }

    public void logNewLeadershipTerm(
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final long maxLogPosition,
        final int leaderMemberId,
        final int logSessionId)
    {
        if (ClusterEventCode.isEnabled(NEW_LEADERSHIP_TERM, ENABLED_EVENT_CODES))
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = ClusterEventEncoder.newLeadershipTerm(
                encodedBuffer,
                logLeadershipTermId,
                logPosition,
                leadershipTermId,
                maxLogPosition,
                leaderMemberId,
                logSessionId);

            ringBuffer.write(toEventCodeId(NEW_LEADERSHIP_TERM), encodedBuffer, 0, encodedLength);
        }
    }

    public void logStateChange(
        final ConsensusModule.State oldState, final ConsensusModule.State newState, final int memberId)
    {
        if (ClusterEventCode.isEnabled(STATE_CHANGE, ENABLED_EVENT_CODES))
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = ClusterEventEncoder.stateChange(encodedBuffer, oldState, newState, memberId);

            ringBuffer.write(toEventCodeId(STATE_CHANGE), encodedBuffer, 0, encodedLength);
        }
    }

    public void logRoleChange(final Cluster.Role oldRole, final Cluster.Role newRole, final int memberId)
    {
        if (ClusterEventCode.isEnabled(ROLE_CHANGE, ENABLED_EVENT_CODES))
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = ClusterEventEncoder.roleChange(encodedBuffer, oldRole, newRole, memberId);

            ringBuffer.write(toEventCodeId(ROLE_CHANGE), encodedBuffer, 0, encodedLength);
        }
    }

    public static int toEventCodeId(final ClusterEventCode code)
    {
        return ClusterEventCode.EVENT_CODE_TYPE << 16 | (code.id() & 0xFFFF);
    }
}

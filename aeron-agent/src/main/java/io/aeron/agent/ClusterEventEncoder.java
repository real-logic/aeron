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

final class ClusterEventEncoder
{
    static int encodeElectionStateChange(
        final MutableDirectBuffer encodedBuffer,
        final Election.State newState,
        final long nowMs)
    {
        final int timestampOffset = encodedBuffer.putStringAscii(0, newState.name());
        encodedBuffer.putLong(timestampOffset, nowMs);
        return timestampOffset + Long.BYTES;
    }

    static int newLeadershipTerm(
        final MutableDirectBuffer encodedBuffer,
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final long maxLogPosition,
        final int leaderMemberId,
        final int logSessionId)
    {
        int offset = 0;
        encodedBuffer.putLong(0, logLeadershipTermId);
        offset += Long.BYTES;
        encodedBuffer.putLong(offset, logPosition);
        offset += Long.BYTES;
        encodedBuffer.putLong(offset, leadershipTermId);
        offset += Long.BYTES;
        encodedBuffer.putLong(offset, maxLogPosition);
        offset += Long.BYTES;
        encodedBuffer.putInt(offset, leaderMemberId);
        offset += Integer.BYTES;
        encodedBuffer.putInt(offset, logSessionId);
        return offset + Integer.BYTES;
    }

    static int stateChange(final MutableDirectBuffer encodedBuffer, final ConsensusModule.State state)
    {
        return encodedBuffer.putStringAscii(0, state.name());
    }

    static int roleChange(final MutableDirectBuffer encodedBuffer, final Cluster.Role role)
    {
        return encodedBuffer.putStringAscii(0, role.name());
    }
}

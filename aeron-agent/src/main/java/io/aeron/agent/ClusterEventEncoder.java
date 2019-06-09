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
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;

final class ClusterEventEncoder
{
    static int encodeElectionStateChange(
        final MutableDirectBuffer encodedBuffer,
        final Election.State oldState,
        final Election.State newState,
        final int memberId)
    {
        int offset = 0;

        encodedBuffer.putInt(offset, memberId);
        offset += BitUtil.SIZE_OF_INT;

        final int stringLength = oldState.name().length() + " -> ".length() + newState.name().length();
        encodedBuffer.putInt(offset, stringLength);
        offset += BitUtil.SIZE_OF_INT;

        offset += encodedBuffer.putStringWithoutLengthAscii(offset, oldState.name());
        offset += encodedBuffer.putStringWithoutLengthAscii(offset, " -> ");
        offset += encodedBuffer.putStringWithoutLengthAscii(offset, newState.name());

        return offset;
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
        offset += BitUtil.SIZE_OF_LONG;
        encodedBuffer.putLong(offset, logPosition);
        offset += BitUtil.SIZE_OF_LONG;
        encodedBuffer.putLong(offset, leadershipTermId);
        offset += BitUtil.SIZE_OF_LONG;
        encodedBuffer.putLong(offset, maxLogPosition);
        offset += BitUtil.SIZE_OF_LONG;
        encodedBuffer.putInt(offset, leaderMemberId);
        offset += BitUtil.SIZE_OF_INT;
        encodedBuffer.putInt(offset, logSessionId);

        return offset + BitUtil.SIZE_OF_INT;
    }

    static int stateChange(
        final MutableDirectBuffer encodedBuffer,
        final ConsensusModule.State oldState,
        final ConsensusModule.State newState,
        final int memberId)
    {
        int offset = 0;

        encodedBuffer.putInt(offset, memberId);
        offset += BitUtil.SIZE_OF_INT;

        final int stringLength = oldState.name().length() + " -> ".length() + newState.name().length();
        encodedBuffer.putInt(offset, stringLength);
        offset += BitUtil.SIZE_OF_INT;

        offset += encodedBuffer.putStringWithoutLengthAscii(offset, oldState.name());
        offset += encodedBuffer.putStringWithoutLengthAscii(offset, " -> ");
        offset += encodedBuffer.putStringWithoutLengthAscii(offset, newState.name());

        return offset;
    }

    static int roleChange(
        final MutableDirectBuffer encodedBuffer,
        final Cluster.Role oldRole,
        final Cluster.Role newRole,
        final int memberId)
    {
        int offset = 0;

        encodedBuffer.putInt(offset, memberId);
        offset += BitUtil.SIZE_OF_INT;

        final int stringLength = oldRole.name().length() + " -> ".length() + newRole.name().length();
        encodedBuffer.putInt(offset, stringLength);
        offset += BitUtil.SIZE_OF_INT;

        offset += encodedBuffer.putStringWithoutLengthAscii(offset, oldRole.name());
        offset += encodedBuffer.putStringWithoutLengthAscii(offset, " -> ");
        offset += encodedBuffer.putStringWithoutLengthAscii(offset, newRole.name());

        return offset;
    }
}

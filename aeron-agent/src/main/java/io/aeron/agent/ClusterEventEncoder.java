/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.agent;

import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.agent.CommonEventEncoder.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

final class ClusterEventEncoder
{
    private ClusterEventEncoder()
    {
    }

    static int encodeNewLeadershipTerm(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long logLeadershipTermId,
        final long nextLeadershipTermId,
        final long nextTermBaseLogPosition,
        final long nextLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int leaderMemberId,
        final int logSessionId,
        final boolean isStartup)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, logLeadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, nextLeadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, nextTermBaseLogPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, nextLogPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, leadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, termBaseLogPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, logPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, leaderRecordingId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, timestamp, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + encodedLength, leaderMemberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, logSessionId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, isStartup ? 1 : 0, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        return encodedLength;
    }

    static int newLeaderShipTermLength()
    {
        return (SIZE_OF_LONG * 9) + (SIZE_OF_INT * 3);
    }

    static <E extends Enum<E>> int encodeStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final E from,
        final E to,
        final int memberId)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + encodedLength, memberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        return encodeTrailingStateChange(encodingBuffer, offset, encodedLength, captureLength, from, to);
    }

    static <E extends Enum<E>> int stateChangeLength(final E from, final E to)
    {
        return stateTransitionStringLength(from, to) + SIZE_OF_INT;
    }

    static <E extends Enum<E>> int encodeElectionStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final E from,
        final E to,
        final int memberId,
        final int leaderId,
        final long candidateTermId,
        final long leadershipTermId,
        final long logPosition,
        final long logLeadershipTermId,
        final long appendPosition,
        final long catchupPosition)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + encodedLength, memberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, leaderId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putLong(offset + encodedLength, candidateTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, leadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, logPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, logLeadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, appendPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, catchupPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        return encodeTrailingStateChange(encodingBuffer, offset, encodedLength, captureLength, from, to);
    }

    static <E extends Enum<E>> int electionStateChangeLength(final E from, final E to)
    {
        return (2 * SIZE_OF_INT) + (6 * SIZE_OF_LONG) + stateTransitionStringLength(from, to);
    }

    static int encodeCanvassPosition(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long logLeadershipTermId,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, logLeadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, leadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, logPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + encodedLength, followerMemberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        return encodedLength;
    }

    static int canvassPositionLength()
    {
        return (3 * SIZE_OF_LONG) + SIZE_OF_INT;
    }

    static int encodeRequestVote(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, logLeadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, logPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, candidateTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + encodedLength, candidateId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        return encodedLength;
    }

    static int requestVoteLength()
    {
        return (3 * SIZE_OF_LONG) + SIZE_OF_INT;
    }
}

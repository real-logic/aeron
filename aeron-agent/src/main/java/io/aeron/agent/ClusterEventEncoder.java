/*
 * Copyright 2014-2022 Real Logic Limited.
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

import io.aeron.cluster.codecs.CloseReason;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

import static io.aeron.agent.CommonEventEncoder.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.*;

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
        final int memberId,
        final int leaderId,
        final int logSessionId,
        final int appVersion,
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

        encodingBuffer.putInt(offset + encodedLength, memberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, leaderId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, logSessionId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, appVersion, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putByte(offset + encodedLength, (byte)(isStartup ? 1 : 0));
        encodedLength += SIZE_OF_BYTE;

        return encodedLength;
    }

    static int newLeaderShipTermLength()
    {
        return (SIZE_OF_LONG * 9) + (SIZE_OF_INT * 4) + SIZE_OF_BYTE;
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

    static int encodeCatchupPosition(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final String catchupEndpoint)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, followerMemberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        bodyLength += encodeTrailingString(
            encodingBuffer, bodyOffset + bodyLength, captureLength - bodyLength, catchupEndpoint);

        return logHeaderLength + bodyLength;
    }

    static int catchupPositionLength(final String endpoint)
    {
        return (2 * SIZE_OF_LONG) + SIZE_OF_INT + SIZE_OF_INT + endpoint.length();
    }

    static int encodeStopCatchup(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int length,
        final int captureLength,
        final long leadershipTermId,
        final int followerMemberId)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, followerMemberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        return logHeaderLength + bodyLength;
    }

    static <E extends Enum<E>> int encodeTruncateLogEntry(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int length,
        final int captureLength,
        final int memberId,
        final E state,
        final long logLeadershipTermId,
        final long leadershipTermId,
        final long candidateTermId,
        final long commitPosition,
        final long logPosition,
        final long appendPosition,
        final long oldPosition,
        final long newPosition)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + encodedLength, memberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putLong(offset + encodedLength, logLeadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, leadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, candidateTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, commitPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, logPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, appendPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, oldPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, newPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodedLength += encodingBuffer.putStringAscii(offset + encodedLength, stateName(state), LITTLE_ENDIAN);

        return encodedLength;
    }

    static int encodeReplayNewLeadershipTermEvent(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final boolean isInElection,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putInt(bodyOffset + bodyLength, isInElection ? 1 : 0, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, timestamp, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, termBaseLogPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, appVersion, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putInt(bodyOffset + bodyLength, timeUnit.name().length(), LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        bodyLength += encodingBuffer.putStringWithoutLengthAscii(bodyOffset + bodyLength, timeUnit.name());

        return logHeaderLength + bodyLength;
    }

    static int replayNewLeadershipTermEventLength(final TimeUnit timeUnit)
    {
        return (2 * SIZE_OF_INT) + (4 * SIZE_OF_LONG) + SIZE_OF_INT + SIZE_OF_INT + timeUnit.name().length();
    }

    static int encodeAppendPosition(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long leadershipTermId,
        final long logPosition,
        final int memberId,
        final short flags)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putByte(bodyOffset + bodyLength, (byte)flags);
        bodyLength += SIZE_OF_BYTE;

        return logHeaderLength + bodyLength;
    }

    static int encodeCommitPosition(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long leadershipTermId,
        final long logPosition,
        final int leaderId,
        final int memberId)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, leaderId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        return logHeaderLength + bodyLength;
    }

    static int addPassiveMemberLength(final String endpoints)
    {
        return SIZE_OF_LONG + 2 * SIZE_OF_INT + endpoints.length();
    }

    static int encodeAddPassiveMember(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long correlationId,
        final String memberEndpoints,
        final int memberId)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, correlationId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        bodyLength += encodeTrailingString(
            encodingBuffer, bodyOffset + bodyLength, captureLength - bodyLength, memberEndpoints);

        return logHeaderLength + bodyLength;
    }

    static int appendSessionCloseLength(final CloseReason closeReason, final TimeUnit timeUnit)
    {
        return SIZE_OF_INT + SIZE_OF_LONG + (SIZE_OF_INT + closeReason.name().length()) + SIZE_OF_LONG +
            SIZE_OF_LONG + (SIZE_OF_INT + timeUnit.name().length());
    }

    static int encodeAppendSessionClose(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long sessionId,
        final CloseReason closeReason,
        final long leadershipTermId,
        final long timestamp,
        final TimeUnit timeUnit)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putLong(bodyOffset + bodyLength, sessionId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, closeReason.name().length(), LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        bodyLength += encodingBuffer.putStringWithoutLengthAscii(bodyOffset + bodyLength, closeReason.name());

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, timestamp, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, timeUnit.name().length(), LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        bodyLength += encodingBuffer.putStringWithoutLengthAscii(bodyOffset + bodyLength, timeUnit.name());

        return logHeaderLength + bodyLength;
    }
}

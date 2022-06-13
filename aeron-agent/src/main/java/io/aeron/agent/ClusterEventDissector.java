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

import io.aeron.protocol.HeaderFlyweight;
import org.agrona.MutableDirectBuffer;

import static io.aeron.agent.ClusterEventCode.ELECTION_STATE_CHANGE;
import static io.aeron.agent.ClusterEventCode.NEW_LEADERSHIP_TERM;
import static io.aeron.agent.CommonEventDissector.dissectLogHeader;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

final class ClusterEventDissector
{
    static final String CONTEXT = "CLUSTER";

    private ClusterEventDissector()
    {
    }

    static void dissectNewLeadershipTerm(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, NEW_LEADERSHIP_TERM, buffer, absoluteOffset, builder);

        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long nextLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long nextTermBaseLogPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long nextLogPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long termBaseLogPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long leaderRecordingId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long timestamp = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final int leaderId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final int logSessionId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final int appVersion = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final boolean isStartup = 1 == buffer.getByte(absoluteOffset);

        builder.append(": memberId=").append(memberId)
            .append(" logLeadershipTermId=").append(logLeadershipTermId)
            .append(" nextLeadershipTermId=").append(nextLeadershipTermId)
            .append(" nextTermBaseLogPosition=").append(nextTermBaseLogPosition)
            .append(" nextLogPosition=").append(nextLogPosition)
            .append(" leadershipTermId=").append(leadershipTermId)
            .append(" termBaseLogPosition=").append(termBaseLogPosition)
            .append(" logPosition=").append(logPosition)
            .append(" leaderRecordingId=").append(leaderRecordingId)
            .append(" timestamp=").append(timestamp)
            .append(" leaderId=").append(leaderId)
            .append(" logSessionId=").append(logSessionId)
            .append(" appVersion=").append(appVersion)
            .append(" isStartup=").append(isStartup);
    }

    static void dissectStateChange(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(' ');
        buffer.getStringAscii(absoluteOffset, builder);
    }

    static void dissectElectionStateChange(
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, ELECTION_STATE_CHANGE, buffer, absoluteOffset, builder);

        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int leaderId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final long candidateTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long appendPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long catchupPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        builder.append(": memberId=").append(memberId).append(' ');
        buffer.getStringAscii(absoluteOffset, builder);
        builder.append(" leaderId=").append(leaderId);
        builder.append(" candidateTermId=").append(candidateTermId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" logLeadershipTermId=").append(logLeadershipTermId);
        builder.append(" appendPosition=").append(appendPosition);
        builder.append(" catchupPosition=").append(catchupPosition);
    }

    static void dissectCanvassPosition(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int followerMemberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);

        builder.append(": logLeadershipTermId=").append(logLeadershipTermId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" followerMemberId=").append(followerMemberId);
    }

    static void dissectRequestVote(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long candidateTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int candidateId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);

        builder.append(": logLeadershipTermId=").append(logLeadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" candidateTermId=").append(candidateTermId);
        builder.append(" candidateId=").append(candidateId);
    }

    static void dissectCatchupPosition(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int catchupEndpointLength = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" catchupEndpoint=");
        buffer.getStringWithoutLengthAscii(absoluteOffset, catchupEndpointLength, builder);
    }

    static void dissectStopCatchup(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);

        builder.append(": memberId=").append(memberId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
    }

    static void dissectTruncateLogEntry(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long candidateTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long commitPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long appendPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long oldPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long newPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        builder.append(": memberId=").append(memberId);
        builder.append(" state=");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
        builder.append(" logLeadershipTermId=").append(logLeadershipTermId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" candidateTermId=").append(candidateTermId);
        builder.append(" commitPosition=").append(commitPosition);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" appendPosition=").append(appendPosition);
        builder.append(" oldPosition=").append(oldPosition);
        builder.append(" newPosition=").append(newPosition);
    }

    public static void dissectReplayNewLeadershipTerm(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final boolean isInElection = 0 != buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long timestamp = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long termBaseLogPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final int appVersion = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final int timeUnitLength = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" isInElection=").append(isInElection);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" termBaseLogPosition=").append(termBaseLogPosition);
        builder.append(" appVersion=").append(appVersion);
        builder.append(" timestamp=").append(timestamp);
        builder.append(" timeUnit=");

        buffer.getStringWithoutLengthAscii(absoluteOffset, timeUnitLength, builder);
    }

    public static void dissectAppendPosition(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final short flags = (short)(buffer.getByte(absoluteOffset) & 0xFF);

        builder.append(": memberId=").append(memberId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" flags=0b");
        HeaderFlyweight.appendFlagsAsChars(flags, builder);
    }

    public static void dissectCommitPosition(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int leaderId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);

        builder.append(": memberId=").append(memberId);
        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" logPosition=").append(logPosition);
        builder.append(" leaderId=").append(leaderId);
    }

    public static void dissectAddPassiveMember(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final long correlationId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" correlationId=").append(correlationId);
        builder.append(" memberEndpoints=");
        buffer.getStringAscii(absoluteOffset, builder, LITTLE_ENDIAN);
    }

    public static void dissectAppendCloseSession(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;
        final long sessionId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int closeReasonLength = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(" sessionId=").append(sessionId);
        builder.append(" closeReason=");
        absoluteOffset += buffer.getStringWithoutLengthAscii(absoluteOffset, closeReasonLength, builder);

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final long timestamp = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;
        final int timeUnitLength = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(" leadershipTermId=").append(leadershipTermId);
        builder.append(" timestamp=").append(timestamp);
        builder.append(" timeUnit=");
        buffer.getStringWithoutLengthAscii(absoluteOffset, timeUnitLength, builder);
    }
}

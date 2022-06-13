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
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.aeron.agent.AgentTests.verifyLogHeader;
import static io.aeron.agent.ClusterEventCode.*;
import static io.aeron.agent.ClusterEventEncoder.*;
import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.EventConfiguration.BUFFER_LENGTH_DEFAULT;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.*;
import static org.agrona.BitUtil.*;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.ALIGNMENT;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.encodedMsgOffset;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TAIL_POSITION_OFFSET;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterEventLoggerTest
{
    private static final int CAPACITY = align(BUFFER_LENGTH_DEFAULT, CACHE_LINE_LENGTH);
    private final UnsafeBuffer logBuffer = new UnsafeBuffer(
        allocateDirectAligned(BUFFER_LENGTH_DEFAULT + TRAILER_LENGTH, CACHE_LINE_LENGTH));
    private final ClusterEventLogger logger = new ClusterEventLogger(new ManyToOneRingBuffer(logBuffer));

    @Test
    void logNewLeadershipTerm()
    {
        final int offset = align(22, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long logLeadershipTermId = 434;
        final long nextLeadershipTermId = 2561;
        final long nextTermBaseLogPosition = 2562;
        final long nextLogPosition = 2563;
        final long leadershipTermId = -500;
        final long logPosition = 43;
        final long timestamp = 2;
        final int memberId = 19;
        final int leaderId = -1;
        final int logSessionId = 3;
        final int appVersion = 777;
        final int captureLength = newLeaderShipTermLength();
        final boolean isStartup = true;
        final long termBaseLogPosition = 982734;
        final long leaderRecordingId = 76434;

        logger.logNewLeadershipTerm(
            logLeadershipTermId,
            nextLeadershipTermId,
            nextTermBaseLogPosition,
            nextLogPosition,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            leaderRecordingId,
            timestamp,
            memberId,
            leaderId,
            logSessionId,
            appVersion,
            isStartup);

        verifyLogHeader(logBuffer, offset, NEW_LEADERSHIP_TERM.toEventCodeId(), captureLength, captureLength);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(logLeadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(nextLeadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(
            nextTermBaseLogPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(nextLogPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(termBaseLogPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leaderRecordingId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(timestamp, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(leaderId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(logSessionId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(appVersion, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;

        assertEquals(isStartup, 1 == logBuffer.getByte(index));
    }

    @Test
    void logStateChange()
    {
        final int offset = ALIGNMENT * 11;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final TimeUnit from = MINUTES;
        final TimeUnit to = SECONDS;
        final int memberId = 42;
        final String payload = from.name() + STATE_SEPARATOR + to.name();
        final int captureLength = SIZE_OF_INT * 2 + payload.length();

        logger.logStateChange(STATE_CHANGE, from, to, memberId);

        verifyLogHeader(logBuffer, offset, STATE_CHANGE.toEventCodeId(), captureLength, captureLength);
        final int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        assertEquals(payload, logBuffer.getStringAscii(index + SIZE_OF_INT));
    }

    @Test
    void logElectionStateChange()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final ChronoUnit from = ChronoUnit.ERAS;
        final ChronoUnit to = null;
        final int memberId = 18;
        final int leaderId = -1;
        final long candidateTermId = 29L;
        final long leadershipTermId = 0L;
        final long logPosition = 100L;
        final long logLeadershipTermId = -9L;
        final long appendPosition = 16 * 1024L;
        final long catchupPosition = 8192L;
        final int length = electionStateChangeLength(from, to);

        logger.logElectionStateChange(
            from,
            to,
            memberId,
            leaderId,
            candidateTermId,
            leadershipTermId,
            logPosition,
            logLeadershipTermId,
            appendPosition,
            catchupPosition);

        verifyLogHeader(logBuffer, offset, ELECTION_STATE_CHANGE.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(leaderId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(candidateTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logLeadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(appendPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(catchupPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(from.name() + STATE_SEPARATOR + "null", logBuffer.getStringAscii(index));
    }

    @Test
    void logCatchupPosition()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long leadershipTermId = 1233L;
        final long logPosition = 100L;
        final int followerMemberId = 18;
        final String catchupEndpoint = "aeron:udp?endpoint=localhost:9090";

        logger.logCatchupPosition(leadershipTermId, logPosition, followerMemberId, catchupEndpoint);

        final int length = (2 * SIZE_OF_LONG) + SIZE_OF_INT + SIZE_OF_INT + catchupEndpoint.length();
        verifyLogHeader(logBuffer, offset, CATCHUP_POSITION.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(followerMemberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        final int catchupEndpointLength = logBuffer.getInt(index, LITTLE_ENDIAN);
        index += SIZE_OF_INT;
        assertEquals(catchupEndpoint, logBuffer.getStringWithoutLengthAscii(index, catchupEndpointLength));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectCatchupPosition(CATCHUP_POSITION, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: CATCHUP_POSITION \\[57/57]: " +
            "memberId=18 leadershipTermId=1233 logPosition=100 catchupEndpoint=aeron:udp\\?endpoint=localhost:9090";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logCatchupPositionLongCatchupEndpoint()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long leadershipTermId = 1233L;
        final long logPosition = 100L;
        final int followerMemberId = -7;

        final byte[] alias = new byte[8192];
        Arrays.fill(alias, (byte)'x');

        final String catchupEndpoint = "aeron:udp?endpoint=localhost:9090|alias=" + new String(
            alias,
            StandardCharsets.US_ASCII);

        logger.logCatchupPosition(leadershipTermId, logPosition, followerMemberId, catchupEndpoint);

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectCatchupPosition(CATCHUP_POSITION, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]*\\.[0-9]*] CLUSTER: CATCHUP_POSITION \\[[0-9]*/8256]: " +
            "memberId=-7 leadershipTermId=1233 logPosition=100 " +
            "catchupEndpoint=aeron:udp\\?endpoint=localhost:9090\\|alias=(x)*\\.\\.\\.";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logStopCatchup()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long leadershipTermId = 1233L;
        final int followerMemberId = 42;

        logger.logStopCatchup(leadershipTermId, followerMemberId);

        final int length = SIZE_OF_LONG + SIZE_OF_INT;
        verifyLogHeader(logBuffer, offset, STOP_CATCHUP.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(followerMemberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectStopCatchup(STOP_CATCHUP, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: STOP_CATCHUP \\[12/12]: " +
            "memberId=42 leadershipTermId=1233";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logTruncateLogEntry()
    {
        final int offset = align(22, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final ChronoUnit state = ChronoUnit.FOREVER;
        final int memberId = 8;
        final long logLeadershipTermId = 777L;
        final long leadershipTermId = 1233L;
        final long candidateTermId = 42L;
        final int commitPosition = 1000;
        final long logPosition = 33L;
        final long appendPosition = 555L;
        final long oldPosition = 98L;
        final long newPosition = 24L;

        logger.logTruncateLogEntry(
            memberId,
            state,
            logLeadershipTermId,
            leadershipTermId,
            candidateTermId,
            commitPosition,
            logPosition,
            appendPosition,
            oldPosition,
            newPosition);

        final int length = SIZE_OF_INT + state.name().length() + SIZE_OF_INT + 8 * SIZE_OF_LONG;
        verifyLogHeader(logBuffer, offset, TRUNCATE_LOG_ENTRY.toEventCodeId(), length, length);

        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(logLeadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(candidateTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(commitPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(appendPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(oldPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(newPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(stateName(state), logBuffer.getStringAscii(index, LITTLE_ENDIAN));
    }

    @Test
    void logReplayNewLeadershipTerm()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final int memberId = 982374;
        final boolean isInElection = true;
        final long leadershipTermId = 1233L;
        final long logPosition = 988723465L;
        final long timestamp = 890723452345L;
        final long termBaseLogPosition = logPosition - 32;
        final TimeUnit timeUnit = NANOSECONDS;
        final int appVersion = 13;

        logger.logReplayNewLeadershipTermEvent(
            memberId,
            isInElection,
            leadershipTermId,
            logPosition,
            timestamp,
            termBaseLogPosition,
            TimeUnit.NANOSECONDS,
            appVersion);

        final int length = replayNewLeadershipTermEventLength(timeUnit);
        verifyLogHeader(logBuffer, offset, REPLAY_NEW_LEADERSHIP_TERM.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;

        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(isInElection, 0 != logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(timestamp, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(termBaseLogPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(appVersion, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(timeUnit.name().length(), logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(timeUnit.name(), logBuffer.getStringWithoutLengthAscii(index, timeUnit.name().length()));
        index += timeUnit.name().length();

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectReplayNewLeadershipTerm(
            REPLAY_NEW_LEADERSHIP_TERM, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: REPLAY_NEW_LEADERSHIP_TERM " +
            "\\[59/59]: memberId=982374 isInElection=true leadershipTermId=1233 logPosition=988723465 " +
            "termBaseLogPosition=988723433 appVersion=13 timestamp=890723452345 timeUnit=NANOSECONDS";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logAppendPosition()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long leadershipTermId = 1233L;
        final long logPosition = 988723465L;
        final int memberId = 982374;
        final byte flags = 1;

        logger.logAppendPosition(leadershipTermId, logPosition, memberId, flags);

        final int length = (2 * SIZE_OF_LONG) + SIZE_OF_INT + SIZE_OF_BYTE;
        verifyLogHeader(logBuffer, offset, APPEND_POSITION.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;

        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(flags, logBuffer.getByte(index));
        index += SIZE_OF_BYTE;

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectAppendPosition(APPEND_POSITION, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: APPEND_POSITION " +
            "\\[21/21]: memberId=982374 leadershipTermId=1233 logPosition=988723465 flags=0b00000001";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logCommitPosition()
    {
        final int offset = ALIGNMENT * 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long leadershipTermId = 1233L;
        final long logPosition = 988723465L;
        final int leaderId = 982374;
        final int memberId = 2;

        logger.logCommitPosition(leadershipTermId, logPosition, leaderId, memberId);

        final int length = 2 * SIZE_OF_LONG + 2 * SIZE_OF_INT;
        verifyLogHeader(logBuffer, offset, COMMIT_POSITION.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;

        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(leaderId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectCommitPosition(COMMIT_POSITION, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: COMMIT_POSITION " +
            "\\[24/24]: memberId=2 leadershipTermId=1233 logPosition=988723465 leaderId=982374";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void testLogAddPassiveMember()
    {
        final int offset = ALIGNMENT + 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long correlationId = 28397456L;
        final String memberEndpoints = "localhost:20113,localhost:20223,localhost:20333,localhost:0,localhost:8013";
        final int memberId = 42;

        logger.logAddPassiveMember(correlationId, memberEndpoints, memberId);

        final int length = SIZE_OF_LONG + 2 * SIZE_OF_INT + memberEndpoints.length();
        verifyLogHeader(logBuffer, offset, ADD_PASSIVE_MEMBER.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;

        assertEquals(correlationId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(memberEndpoints.length(), logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(memberEndpoints, logBuffer.getStringWithoutLengthAscii(index, memberEndpoints.length()));
        index += memberEndpoints.length();

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectAddPassiveMember(
            ADD_PASSIVE_MEMBER, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: ADD_PASSIVE_MEMBER " +
            "\\[90/90]: memberId=42 correlationId=28397456 " +
            "memberEndpoints=localhost:20113,localhost:20223,localhost:20333,localhost:0,localhost:8013";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void testLogAppendSessionClose()
    {
        final int offset = ALIGNMENT + 4;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);

        final int memberId = 829374;
        final long sessionId = 289374L;
        final CloseReason closeReason = CloseReason.TIMEOUT;
        final long leadershipTermId = 2039842L;
        final long timestamp = 29384;
        final TimeUnit timeUnit = MILLISECONDS;

        logger.logAppendSessionClose(memberId, sessionId, closeReason, leadershipTermId, timestamp, timeUnit);

        final int length = SIZE_OF_INT + SIZE_OF_LONG + (SIZE_OF_INT + closeReason.name().length()) + SIZE_OF_LONG +
            SIZE_OF_LONG + (SIZE_OF_INT + timeUnit.name().length());

        verifyLogHeader(logBuffer, offset, APPEND_SESSION_CLOSE.toEventCodeId(), length, length);
        int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;

        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(sessionId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(closeReason.name().length(), logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(closeReason.name(), logBuffer.getStringWithoutLengthAscii(index, closeReason.name().length()));
        index += closeReason.name().length();
        assertEquals(leadershipTermId, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(timestamp, logBuffer.getLong(index, LITTLE_ENDIAN));
        index += SIZE_OF_LONG;
        assertEquals(timeUnit.name().length(), logBuffer.getInt(index, LITTLE_ENDIAN));
        index += SIZE_OF_INT;
        assertEquals(timeUnit.name(), logBuffer.getStringWithoutLengthAscii(index, timeUnit.name().length()));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectAppendCloseSession(
            APPEND_SESSION_CLOSE, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: APPEND_SESSION_CLOSE " +
            "\\[55/55]: memberId=829374 sessionId=289374 closeReason=TIMEOUT leadershipTermId=2039842 " +
            "timestamp=29384 timeUnit=MILLISECONDS";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));
    }

    @Test
    void logDynamicJoinStateChange()
    {
        final int offset = ALIGNMENT * 11;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final TimeUnit from = MINUTES;
        final TimeUnit to = SECONDS;
        final int memberId = 42;
        final String payload = from.name() + STATE_SEPARATOR + to.name();
        final int captureLength = SIZE_OF_INT * 2 + payload.length();

        logger.logStateChange(DYNAMIC_JOIN_STATE_CHANGE, from, to, memberId);

        verifyLogHeader(logBuffer, offset, DYNAMIC_JOIN_STATE_CHANGE.toEventCodeId(), captureLength, captureLength);
        final int index = encodedMsgOffset(offset) + LOG_HEADER_LENGTH;
        assertEquals(memberId, logBuffer.getInt(index, LITTLE_ENDIAN));
        assertEquals(payload, logBuffer.getStringAscii(index + SIZE_OF_INT));

        final StringBuilder sb = new StringBuilder();
        ClusterEventDissector.dissectStateChange(
            DYNAMIC_JOIN_STATE_CHANGE, logBuffer, encodedMsgOffset(offset), sb);

        final String expectedMessagePattern = "\\[[0-9]+\\.[0-9]+] CLUSTER: DYNAMIC_JOIN_STATE_CHANGE " +
            "\\[26/26]: memberId=42 MINUTES -> SECONDS";

        assertThat(sb.toString(), Matchers.matchesPattern(expectedMessagePattern));

    }
}

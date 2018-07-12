/*
 *  Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.cluster;

import io.aeron.Publication;
import io.aeron.cluster.codecs.*;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.ExpandableArrayBuffer;

import java.util.ArrayList;

import static io.aeron.Aeron.NULL_VALUE;

class MemberStatusPublisher
{
    private static final int SEND_ATTEMPTS = 3;

    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final CanvassPositionEncoder canvassPositionEncoder = new CanvassPositionEncoder();
    private final RequestVoteEncoder requestVoteEncoder = new RequestVoteEncoder();
    private final VoteEncoder voteEncoder = new VoteEncoder();
    private final NewLeadershipTermEncoder newLeadershipTermEncoder = new NewLeadershipTermEncoder();
    private final AppendedPositionEncoder appendedPositionEncoder = new AppendedPositionEncoder();
    private final CommitPositionEncoder commitPositionEncoder = new CommitPositionEncoder();
    private final CatchupPositionEncoder catchupPositionEncoder = new CatchupPositionEncoder();
    private final StopCatchupEncoder stopCatchupEncoder = new StopCatchupEncoder();
    private final RecoveryPlanQueryEncoder recoveryPlanQueryEncoder = new RecoveryPlanQueryEncoder();
    private final RecoveryPlanEncoder recoveryPlanEncoder = new RecoveryPlanEncoder();
    private final RecordingLogQueryEncoder recordingLogQueryEncoder = new RecordingLogQueryEncoder();
    private final RecordingLogEncoder recordingLogEncoder = new RecordingLogEncoder();

    boolean canvassPosition(
        final Publication publication,
        final long logLeadershipTermId,
        final long logPosition,
        final int followerMemberId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + CanvassPositionEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                canvassPositionEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .logLeadershipTermId(logLeadershipTermId)
                    .logPosition(logPosition)
                    .followerMemberId(followerMemberId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean requestVote(
        final Publication publication,
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateMemberId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + RequestVoteEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                requestVoteEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .logLeadershipTermId(logLeadershipTermId)
                    .logPosition(logPosition)
                    .candidateTermId(candidateTermId)
                    .candidateMemberId(candidateMemberId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean placeVote(
        final Publication publication,
        final long candidateTermId,
        final long logLeadershipTermId,
        final long logPosition,
        final int candidateMemberId,
        final int followerMemberId,
        final boolean vote)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + VoteEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                voteEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .candidateTermId(candidateTermId)
                    .logLeadershipTermId(logLeadershipTermId)
                    .logPosition(logPosition)
                    .candidateMemberId(candidateMemberId)
                    .followerMemberId(followerMemberId)
                    .vote(vote ? BooleanType.TRUE : BooleanType.FALSE);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean newLeadershipTerm(
        final Publication publication,
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int leaderMemberId,
        final int logSessionId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + NewLeadershipTermEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                newLeadershipTermEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .logLeadershipTermId(logLeadershipTermId)
                    .logPosition(logPosition)
                    .leadershipTermId(leadershipTermId)
                    .leaderMemberId(leaderMemberId)
                    .logSessionId(logSessionId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean appendedPosition(
        final Publication publication, final long leadershipTermId, final long logPosition, final int followerMemberId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + AppendedPositionEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                appendedPositionEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .followerMemberId(followerMemberId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean commitPosition(
        final Publication publication, final long leadershipTermId, final long logPosition, final int leaderMemberId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + CommitPositionEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                commitPositionEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .leaderMemberId(leaderMemberId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean catchupPosition(
        final Publication publication, final long leadershipTermId, final long logPosition, final int followerMemerId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + CatchupPositionEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                catchupPositionEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .followerMemberId(followerMemerId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean stopCatchup(
        final Publication publication, final int replaySessionId, final int followerMemerId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + StopCatchupEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                stopCatchupEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .replaySessionId(replaySessionId)
                    .followerMemberId(followerMemerId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean recoveryPlanQuery(
        final Publication publication, final long correlationId, final int leaderMemberId, final int memberId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + RecoveryPlanQueryEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                recoveryPlanQueryEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .leaderMemberId(leaderMemberId)
                    .requestMemberId(memberId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean recoveryPlan(
        final Publication publication,
        final long correlationId,
        final int requestMemberId,
        final int leaderMemberId,
        final RecordingLog.RecoveryPlan recoveryPlan)
    {
        recoveryPlanEncoder.wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .requestMemberId(requestMemberId)
            .leaderMemberId(leaderMemberId);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + recoveryPlan.encode(recoveryPlanEncoder);

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.offer(buffer, 0, length);
            if (result > 0)
            {
                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean recordingLogQuery(
        final Publication publication,
        final long correlationId,
        final int leaderMemberId,
        final int memberId,
        final long fromLeadershipTermId,
        final int count,
        final boolean includeSnapshots)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + RecordingLogQueryEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                recordingLogQueryEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .leaderMemberId(leaderMemberId)
                    .requestMemberId(memberId)
                    .fromLeadershipTermId(fromLeadershipTermId)
                    .count(count)
                    .includeSnapshots(includeSnapshots ? BooleanType.TRUE : BooleanType.FALSE);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean recordingLog(
        final Publication publication,
        final long correlationId,
        final int requestMemberId,
        final int leaderMemberId,
        final RecordingLog recordingLog,
        final long fromLeadershipTermId,
        final int count,
        final boolean includeSnapshots)
    {
        recordingLogEncoder.wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .requestMemberId(requestMemberId)
            .leaderMemberId(leaderMemberId);

        final ArrayList<RecordingLog.Entry> results = new ArrayList<>();
        recordingLog.findEntries(fromLeadershipTermId, count, includeSnapshots, results);

        final int resultsSize = results.size();
        final RecordingLogEncoder.EntriesEncoder entriesEncoder = recordingLogEncoder.entriesCount(resultsSize);
        for (int i = 0; i < resultsSize; i++)
        {
            final RecordingLog.Entry entry = results.get(i);

            entriesEncoder.next()
                .recordingId(entry.recordingId)
                .leadershipTermId(entry.leadershipTermId)
                .termBaseLogPosition(entry.termBaseLogPosition)
                .logPosition(entry.logPosition)
                .timestamp(entry.timestamp)
                .serviceId(entry.serviceId)
                .entryType(entry.type)
                .isCurrent(NULL_VALUE == entry.logPosition ? BooleanType.TRUE : BooleanType.FALSE);
        }

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + recordingLogEncoder.encodedLength();

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.offer(buffer, 0, length);
            if (result > 0)
            {
                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new AeronException("unexpected publication state: " + result);
        }
    }
}

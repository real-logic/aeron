/*
 *  Copyright 2014-2019 Real Logic Ltd.
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
    private final AddPassiveMemberEncoder addPassiveMemberEncoder = new AddPassiveMemberEncoder();
    private final ClusterMembersChangeEncoder clusterMembersChangeEncoder = new ClusterMembersChangeEncoder();
    private final SnapshotRecordingQueryEncoder snapshotRecordingQueryEncoder = new SnapshotRecordingQueryEncoder();
    private final SnapshotRecordingsEncoder snapshotRecordingsEncoder = new SnapshotRecordingsEncoder();
    private final JoinClusterEncoder joinClusterEncoder = new JoinClusterEncoder();
    private final TerminationPositionEncoder terminationPositionEncoder = new TerminationPositionEncoder();
    private final TerminationAckEncoder terminationAckEncoder = new TerminationAckEncoder();

    void canvassPosition(
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

                return;
            }

            checkResult(result);
        }
        while (--attempts > 0);
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

    void placeVote(
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

                return;
            }

            checkResult(result);
        }
        while (--attempts > 0);
    }

    void newLeadershipTerm(
        final Publication publication,
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final long maxLogPosition,
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
                    .maxLogPosition(maxLogPosition)
                    .leaderMemberId(leaderMemberId)
                    .logSessionId(logSessionId);

                bufferClaim.commit();

                return;
            }

            checkResult(result);
        }
        while (--attempts > 0);
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

    void commitPosition(
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

                return;
            }

            checkResult(result);
        }
        while (--attempts > 0);
    }

    boolean catchupPosition(
        final Publication publication, final long leadershipTermId, final long logPosition, final int followerMemberId)
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
                    .followerMemberId(followerMemberId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean stopCatchup(
        final Publication publication, final long leadershipTermId, final long logPosition, final int followerMemberId)
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

    boolean addPassiveMember(final Publication publication, final long correlationId, final String memberEndpoints)
    {
        final int length =
            MessageHeaderEncoder.ENCODED_LENGTH +
            AddPassiveMemberEncoder.BLOCK_LENGTH +
            AddPassiveMemberEncoder.memberEndpointsHeaderLength() +
            memberEndpoints.length();

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                addPassiveMemberEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .memberEndpoints(memberEndpoints);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean clusterMemberChange(
        final Publication publication,
        final long correlationId,
        final int leaderMemberId,
        final String activeMembers,
        final String passiveMembers)
    {
        final int length =
            MessageHeaderEncoder.ENCODED_LENGTH +
            ClusterMembersChangeEncoder.BLOCK_LENGTH +
            ClusterMembersChangeEncoder.activeMembersHeaderLength() +
            activeMembers.length() +
            ClusterMembersChangeEncoder.passiveMembersHeaderLength() +
            passiveMembers.length();

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                clusterMembersChangeEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .leaderMemberId(leaderMemberId)
                    .activeMembers(activeMembers)
                    .passiveMembers(passiveMembers);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean snapshotRecordingQuery(final Publication publication, final long correlationId, final int requestMemberId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + SnapshotRecordingQueryEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                snapshotRecordingQueryEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .requestMemberId(requestMemberId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    void snapshotRecording(
        final Publication publication,
        final long correlationId,
        final RecordingLog.RecoveryPlan recoveryPlan,
        final String memberEndpoints)
    {
        snapshotRecordingsEncoder.wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId);

        final SnapshotRecordingsEncoder.SnapshotsEncoder snapshotsEncoder =
            snapshotRecordingsEncoder.snapshotsCount(recoveryPlan.snapshots.size());
        for (int i = 0, length = recoveryPlan.snapshots.size(); i < length; i++)
        {
            final RecordingLog.Snapshot snapshot = recoveryPlan.snapshots.get(i);

            snapshotsEncoder.next()
                .recordingId(snapshot.recordingId)
                .leadershipTermId(snapshot.leadershipTermId)
                .termBaseLogPosition(snapshot.termBaseLogPosition)
                .logPosition(snapshot.logPosition)
                .timestamp(snapshot.timestamp)
                .serviceId(snapshot.serviceId);
        }

        snapshotRecordingsEncoder.memberEndpoints(memberEndpoints);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + snapshotRecordingsEncoder.encodedLength();

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.offer(buffer, 0, length);
            if (result > 0)
            {
                return;
            }

            checkResult(result);
        }
        while (--attempts > 0);
    }

    boolean joinCluster(final Publication publication, final long leadershipTermId, final int memberId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + JoinClusterEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                joinClusterEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .memberId(memberId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean terminationPosition(
        final Publication publication,
        final long logPosition)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + TerminationPositionEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                terminationPositionEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .logPosition(logPosition);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean terminationAck(
        final Publication publication,
        final long logPosition,
        final int memberId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + TerminationAckEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                terminationAckEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .logPosition(logPosition)
                    .memberId(memberId);

                bufferClaim.commit();

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

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
package io.aeron.cluster;

import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.ExpandableArrayBuffer;

final class ConsensusPublisher
{
    private static final int SEND_ATTEMPTS = 3;

    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final CanvassPositionEncoder canvassPositionEncoder = new CanvassPositionEncoder();
    private final RequestVoteEncoder requestVoteEncoder = new RequestVoteEncoder();
    private final VoteEncoder voteEncoder = new VoteEncoder();
    private final NewLeadershipTermEncoder newLeadershipTermEncoder = new NewLeadershipTermEncoder();
    private final AppendPositionEncoder appendPositionEncoder = new AppendPositionEncoder();
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
    private final BackupQueryEncoder backupQueryEncoder = new BackupQueryEncoder();
    private final BackupResponseEncoder backupResponseEncoder = new BackupResponseEncoder();
    private final HeartbeatRequestEncoder heartbeatRequestEncoder = new HeartbeatRequestEncoder();
    private final HeartbeatResponseEncoder heartbeatResponseEncoder = new HeartbeatResponseEncoder();

    void canvassPosition(
        final ExclusivePublication publication,
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int followerMemberId)
    {
        if (null == publication)
        {
            return;
        }

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
                    .leadershipTermId(leadershipTermId)
                    .followerMemberId(followerMemberId)
                    .protocolVersion(ConsensusModule.Configuration.PROTOCOL_SEMANTIC_VERSION);

                bufferClaim.commit();

                return;
            }

            checkResult(result);
        }
        while (--attempts > 0);
    }

    boolean requestVote(
        final ExclusivePublication publication,
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateMemberId)
    {
        if (null == publication)
        {
            return false;
        }

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
                    .candidateMemberId(candidateMemberId)
                    .protocolVersion(ConsensusModule.Configuration.PROTOCOL_SEMANTIC_VERSION);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    void placeVote(
        final ExclusivePublication publication,
        final long candidateTermId,
        final long logLeadershipTermId,
        final long logPosition,
        final int candidateMemberId,
        final int followerMemberId,
        final boolean vote)
    {
        if (null == publication)
        {
            return;
        }

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
        final ExclusivePublication publication,
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
        if (null == publication)
        {
            return;
        }

        if (CommonContext.NULL_SESSION_ID == logSessionId)
        {
            throw new ClusterException("logSessionId was null, should always have a value");
        }

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
                    .nextLeadershipTermId(nextLeadershipTermId)
                    .nextTermBaseLogPosition(nextTermBaseLogPosition)
                    .nextLogPosition(nextLogPosition)
                    .leadershipTermId(leadershipTermId)
                    .termBaseLogPosition(termBaseLogPosition)
                    .logPosition(logPosition)
                    .leaderRecordingId(leaderRecordingId)
                    .timestamp(timestamp)
                    .leaderMemberId(leaderMemberId)
                    .logSessionId(logSessionId)
                    .isStartup(isStartup ? BooleanType.TRUE : BooleanType.FALSE);

                bufferClaim.commit();

                return;
            }

            checkResult(result);
        }
        while (--attempts > 0);
    }

    boolean appendPosition(
        final ExclusivePublication publication,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final short flags)
    {
        if (null == publication)
        {
            return false;
        }

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + AppendPositionEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                appendPositionEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .followerMemberId(followerMemberId)
                    .flags(flags);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    void commitPosition(
        final ExclusivePublication publication,
        final long leadershipTermId,
        final long logPosition,
        final int leaderMemberId)
    {
        if (null == publication)
        {
            return;
        }

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
        final ExclusivePublication publication,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final String catchupEndpoint)
    {
        if (null == publication)
        {
            return false;
        }

        final int length =
            MessageHeaderEncoder.ENCODED_LENGTH +
            CatchupPositionEncoder.BLOCK_LENGTH +
            CatchupPositionEncoder.catchupEndpointHeaderLength() +
            catchupEndpoint.length();

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
                    .followerMemberId(followerMemberId)
                    .catchupEndpoint(catchupEndpoint);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean stopCatchup(final ExclusivePublication publication, final long leadershipTermId, final int followerMemberId)
    {
        if (null == publication)
        {
            return false;
        }

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
                    .followerMemberId(followerMemberId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean addPassiveMember(
        final ExclusivePublication publication, final long correlationId, final String memberEndpoints)
    {
        if (null == publication)
        {
            return true;
        }

        addPassiveMemberEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .memberEndpoints(memberEndpoints);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + addPassiveMemberEncoder.encodedLength();
        return sendPublication(publication, buffer, length);
    }

    boolean clusterMemberChange(
        final ExclusivePublication publication,
        final long correlationId,
        final int leaderMemberId,
        final String activeMembers,
        final String passiveMembers)
    {
        if (null == publication)
        {
            return true;
        }

        clusterMembersChangeEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .leaderMemberId(leaderMemberId)
            .activeMembers(activeMembers)
            .passiveMembers(passiveMembers);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + clusterMembersChangeEncoder.encodedLength();
        return sendPublication(publication, buffer, length);
    }

    boolean snapshotRecordingQuery(
        final ExclusivePublication publication, final long correlationId, final int requestMemberId)
    {
        if (null == publication)
        {
            return false;
        }

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
        final ExclusivePublication publication,
        final long correlationId,
        final RecordingLog.RecoveryPlan recoveryPlan,
        final String memberEndpoints)
    {
        if (null == publication)
        {
            return;
        }

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

    boolean joinCluster(final ExclusivePublication publication, final long leadershipTermId, final int memberId)
    {
        if (null == publication)
        {
            return false;
        }

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
        final ExclusivePublication publication, final long leadershipTermId, final long logPosition)
    {
        if (null == publication)
        {
            return false;
        }

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + TerminationPositionEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                terminationPositionEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
            Thread.yield();
        }
        while (--attempts > 0);

        return false;
    }

    boolean terminationAck(
        final ExclusivePublication publication, final long leadershipTermId, final long logPosition, final int memberId)
    {
        if (null == publication)
        {
            return false;
        }

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + TerminationAckEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                terminationAckEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .memberId(memberId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
            Thread.yield();
        }
        while (--attempts > 0);

        return false;
    }

    boolean backupQuery(
        final ExclusivePublication publication,
        final long correlationId,
        final int responseStreamId,
        final int version,
        final String responseChannel,
        final byte[] encodedCredentials)
    {
        if (null == publication)
        {
            return false;
        }

        backupQueryEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .version(version)
            .responseChannel(responseChannel)
            .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + backupQueryEncoder.encodedLength();
        return sendPublication(publication, buffer, length);
    }

    boolean backupResponse(
        final ClusterSession session,
        final int commitPositionCounterId,
        final int leaderMemberId,
        final RecordingLog.Entry lastEntry,
        final RecordingLog.RecoveryPlan recoveryPlan,
        final String clusterMembers)
    {
        backupResponseEncoder.wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(session.correlationId())
            .logRecordingId(recoveryPlan.log.recordingId)
            .logLeadershipTermId(recoveryPlan.log.leadershipTermId)
            .logTermBaseLogPosition(recoveryPlan.log.termBaseLogPosition)
            .lastLeadershipTermId(lastEntry.leadershipTermId)
            .lastTermBaseLogPosition(lastEntry.termBaseLogPosition)
            .commitPositionCounterId(commitPositionCounterId)
            .leaderMemberId(leaderMemberId);

        final BackupResponseEncoder.SnapshotsEncoder snapshotsEncoder =
            backupResponseEncoder.snapshotsCount(recoveryPlan.snapshots.size());
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

        backupResponseEncoder.clusterMembers(clusterMembers);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + backupResponseEncoder.encodedLength();
        return sendSession(session, buffer, length);
    }

    boolean heartbeatRequest(
        final ExclusivePublication publication,
        final long correlationId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedCredentials)
    {
        if (null == publication)
        {
            return false;
        }

        heartbeatRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .responseChannel(responseChannel)
            .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + heartbeatRequestEncoder.encodedLength();
        return sendPublication(publication, buffer, length);
    }

    public boolean heartbeatResponse(final ClusterSession session)
    {
        heartbeatResponseEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(session.correlationId());

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + heartbeatResponseEncoder.encodedLength();
        return sendSession(session, buffer, length);
    }

    public boolean challengeResponse(
        final ExclusivePublication publication,
        final long nextCorrelationId,
        final long clusterSessionId,
        final byte[] encodedChallengeResponse)
    {
        final ChallengeResponseEncoder encoder = new ChallengeResponseEncoder()
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(nextCorrelationId)
            .clusterSessionId(clusterSessionId)
            .putEncodedCredentials(encodedChallengeResponse, 0, encodedChallengeResponse.length);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();

        return sendPublication(publication, buffer, length);
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new AeronException("unexpected publication state: " + result);
        }
    }

    private static boolean sendPublication(
        final ExclusivePublication publication,
        final ExpandableArrayBuffer buffer,
        final int length)
    {
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

    private static boolean sendSession(
        final ClusterSession session,
        final ExpandableArrayBuffer buffer,
        final int length)
    {
        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = session.responsePublication().offer(buffer, 0, length);
            if (result > 0)
            {
                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }
}

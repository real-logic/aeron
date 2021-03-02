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
package io.aeron.cluster;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

class ConsensusAdapter implements FragmentHandler, AutoCloseable
{
    static final int FRAGMENT_LIMIT = 10;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final CanvassPositionDecoder canvassPositionDecoder = new CanvassPositionDecoder();
    private final RequestVoteDecoder requestVoteDecoder = new RequestVoteDecoder();
    private final VoteDecoder voteDecoder = new VoteDecoder();
    private final NewLeadershipTermDecoder newLeadershipTermDecoder = new NewLeadershipTermDecoder();
    private final AppendPositionDecoder appendPositionDecoder = new AppendPositionDecoder();
    private final CommitPositionDecoder commitPositionDecoder = new CommitPositionDecoder();
    private final CatchupPositionDecoder catchupPositionDecoder = new CatchupPositionDecoder();
    private final StopCatchupDecoder stopCatchupDecoder = new StopCatchupDecoder();

    private final AddPassiveMemberDecoder addPassiveMemberDecoder = new AddPassiveMemberDecoder();
    private final ClusterMembersChangeDecoder clusterMembersChangeDecoder = new ClusterMembersChangeDecoder();
    private final SnapshotRecordingQueryDecoder snapshotRecordingQueryDecoder = new SnapshotRecordingQueryDecoder();
    private final SnapshotRecordingsDecoder snapshotRecordingsDecoder = new SnapshotRecordingsDecoder();
    private final JoinClusterDecoder joinClusterDecoder = new JoinClusterDecoder();
    private final TerminationPositionDecoder terminationPositionDecoder = new TerminationPositionDecoder();
    private final TerminationAckDecoder terminationAckDecoder = new TerminationAckDecoder();
    private final BackupQueryDecoder backupQueryDecoder = new BackupQueryDecoder();

    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this);
    private final Subscription subscription;
    private final ConsensusModuleAgent consensusModuleAgent;

    ConsensusAdapter(final Subscription subscription, final ConsensusModuleAgent consensusModuleAgent)
    {
        this.subscription = subscription;
        this.consensusModuleAgent = consensusModuleAgent;
    }

    public void close()
    {
        CloseHelper.close(subscription);
    }

    public int poll()
    {
        return subscription.poll(fragmentAssembler, FRAGMENT_LIMIT);
    }

    public int poll(final int limit)
    {
        return subscription.poll(fragmentAssembler, limit);
    }

    @SuppressWarnings("MethodLength")
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        switch (messageHeaderDecoder.templateId())
        {
            case CanvassPositionDecoder.TEMPLATE_ID:
                canvassPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onCanvassPosition(
                    canvassPositionDecoder.logLeadershipTermId(),
                    canvassPositionDecoder.logPosition(),
                    canvassPositionDecoder.followerMemberId());
                break;

            case RequestVoteDecoder.TEMPLATE_ID:
                requestVoteDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onRequestVote(
                    requestVoteDecoder.logLeadershipTermId(),
                    requestVoteDecoder.logPosition(),
                    requestVoteDecoder.candidateTermId(),
                    requestVoteDecoder.candidateMemberId());
                break;

            case VoteDecoder.TEMPLATE_ID:
                voteDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onVote(
                    voteDecoder.candidateTermId(),
                    voteDecoder.logLeadershipTermId(),
                    voteDecoder.logPosition(),
                    voteDecoder.candidateMemberId(),
                    voteDecoder.followerMemberId(),
                    voteDecoder.vote() == BooleanType.TRUE);
                break;

            case NewLeadershipTermDecoder.TEMPLATE_ID:
                newLeadershipTermDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onNewLeadershipTerm(
                    newLeadershipTermDecoder.logLeadershipTermId(),
                    newLeadershipTermDecoder.logTruncatePosition(),
                    newLeadershipTermDecoder.leadershipTermId(),
                    newLeadershipTermDecoder.logPosition(),
                    newLeadershipTermDecoder.timestamp(),
                    newLeadershipTermDecoder.leaderMemberId(),
                    newLeadershipTermDecoder.logSessionId(),
                    newLeadershipTermDecoder.isStartup() == BooleanType.TRUE);
                break;

            case AppendPositionDecoder.TEMPLATE_ID:
                appendPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onAppendPosition(
                    appendPositionDecoder.leadershipTermId(),
                    appendPositionDecoder.logPosition(),
                    appendPositionDecoder.followerMemberId());
                break;

            case CommitPositionDecoder.TEMPLATE_ID:
                commitPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onCommitPosition(
                    commitPositionDecoder.leadershipTermId(),
                    commitPositionDecoder.logPosition(),
                    commitPositionDecoder.leaderMemberId());
                break;

            case CatchupPositionDecoder.TEMPLATE_ID:
                catchupPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onCatchupPosition(
                    catchupPositionDecoder.leadershipTermId(),
                    catchupPositionDecoder.logPosition(),
                    catchupPositionDecoder.followerMemberId(),
                    catchupPositionDecoder.catchupEndpoint());
                break;

            case StopCatchupDecoder.TEMPLATE_ID:
                stopCatchupDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onStopCatchup(
                    stopCatchupDecoder.leadershipTermId(),
                    stopCatchupDecoder.followerMemberId());
                break;

            case AddPassiveMemberDecoder.TEMPLATE_ID:
                addPassiveMemberDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onAddPassiveMember(
                    addPassiveMemberDecoder.correlationId(), addPassiveMemberDecoder.memberEndpoints());
                break;

            case ClusterMembersChangeDecoder.TEMPLATE_ID:
                clusterMembersChangeDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onClusterMembersChange(
                    clusterMembersChangeDecoder.correlationId(),
                    clusterMembersChangeDecoder.leaderMemberId(),
                    clusterMembersChangeDecoder.activeMembers(),
                    clusterMembersChangeDecoder.passiveMembers());
                break;

            case SnapshotRecordingQueryDecoder.TEMPLATE_ID:
                snapshotRecordingQueryDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onSnapshotRecordingQuery(
                    snapshotRecordingQueryDecoder.correlationId(), snapshotRecordingQueryDecoder.requestMemberId());
                break;

            case SnapshotRecordingsDecoder.TEMPLATE_ID:
                snapshotRecordingsDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onSnapshotRecordings(
                    snapshotRecordingsDecoder.correlationId(), snapshotRecordingsDecoder);
                break;

            case JoinClusterDecoder.TEMPLATE_ID:
                joinClusterDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onJoinCluster(
                    joinClusterDecoder.leadershipTermId(), joinClusterDecoder.memberId());
                break;

            case TerminationPositionDecoder.TEMPLATE_ID:
                terminationPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onTerminationPosition(
                    terminationPositionDecoder.leadershipTermId(),
                    terminationPositionDecoder.logPosition());
                break;

            case TerminationAckDecoder.TEMPLATE_ID:
                terminationAckDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onTerminationAck(
                    terminationAckDecoder.leadershipTermId(),
                    terminationAckDecoder.logPosition(),
                    terminationAckDecoder.memberId());
                break;

            case BackupQueryDecoder.TEMPLATE_ID:
                backupQueryDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final String responseChannel = backupQueryDecoder.responseChannel();
                final byte[] credentials = new byte[backupQueryDecoder.encodedCredentialsLength()];
                backupQueryDecoder.getEncodedCredentials(credentials, 0, credentials.length);

                consensusModuleAgent.onBackupQuery(
                    backupQueryDecoder.correlationId(),
                    backupQueryDecoder.responseStreamId(),
                    backupQueryDecoder.version(),
                    responseChannel,
                    credentials);
                break;
        }
    }
}

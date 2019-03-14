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
package io.aeron.cluster;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

class MemberStatusAdapter implements FragmentHandler, AutoCloseable
{
    private static final int FRAGMENT_POLL_LIMIT = 10;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final CanvassPositionDecoder canvassPositionDecoder = new CanvassPositionDecoder();
    private final RequestVoteDecoder requestVoteDecoder = new RequestVoteDecoder();
    private final VoteDecoder voteDecoder = new VoteDecoder();
    private final NewLeadershipTermDecoder newLeadershipTermDecoder = new NewLeadershipTermDecoder();
    private final AppendedPositionDecoder appendedPositionDecoder = new AppendedPositionDecoder();
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

    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this);
    private final Subscription subscription;
    private final MemberStatusListener memberStatusListener;

    MemberStatusAdapter(final Subscription subscription, final MemberStatusListener memberStatusListener)
    {
        this.subscription = subscription;
        this.memberStatusListener = memberStatusListener;
    }

    public void close()
    {
        CloseHelper.close(subscription);
    }

    public int poll()
    {
        return subscription.poll(fragmentAssembler, FRAGMENT_POLL_LIMIT);
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

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case CanvassPositionDecoder.TEMPLATE_ID:
                canvassPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onCanvassPosition(
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

                memberStatusListener.onRequestVote(
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

                memberStatusListener.onVote(
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

                memberStatusListener.onNewLeadershipTerm(
                    newLeadershipTermDecoder.logLeadershipTermId(),
                    newLeadershipTermDecoder.logPosition(),
                    newLeadershipTermDecoder.leadershipTermId(),
                    newLeadershipTermDecoder.maxLogPosition(),
                    newLeadershipTermDecoder.leaderMemberId(),
                    newLeadershipTermDecoder.logSessionId());
                break;

            case AppendedPositionDecoder.TEMPLATE_ID:
                appendedPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onAppendedPosition(
                    appendedPositionDecoder.leadershipTermId(),
                    appendedPositionDecoder.logPosition(),
                    appendedPositionDecoder.followerMemberId());
                break;

            case CommitPositionDecoder.TEMPLATE_ID:
                commitPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onCommitPosition(
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

                memberStatusListener.onCatchupPosition(
                    catchupPositionDecoder.leadershipTermId(),
                    catchupPositionDecoder.logPosition(),
                    catchupPositionDecoder.followerMemberId());
                break;

            case StopCatchupDecoder.TEMPLATE_ID:
                stopCatchupDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onStopCatchup(
                    stopCatchupDecoder.leadershipTermId(),
                    stopCatchupDecoder.logPosition(),
                    stopCatchupDecoder.followerMemberId());
                break;

            case AddPassiveMemberDecoder.TEMPLATE_ID:
                addPassiveMemberDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onAddPassiveMember(
                    addPassiveMemberDecoder.correlationId(), addPassiveMemberDecoder.memberEndpoints());
                break;

            case ClusterMembersChangeDecoder.TEMPLATE_ID:
                clusterMembersChangeDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onClusterMembersChange(
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

                memberStatusListener.onSnapshotRecordingQuery(
                    snapshotRecordingQueryDecoder.correlationId(), snapshotRecordingQueryDecoder.requestMemberId());
                break;

            case SnapshotRecordingsDecoder.TEMPLATE_ID:
                snapshotRecordingsDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onSnapshotRecordings(
                    snapshotRecordingsDecoder.correlationId(), snapshotRecordingsDecoder);
                break;

            case JoinClusterDecoder.TEMPLATE_ID:
                joinClusterDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onJoinCluster(
                    joinClusterDecoder.leadershipTermId(), joinClusterDecoder.memberId());
                break;

            case TerminationPositionDecoder.TEMPLATE_ID:
                terminationPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onTerminationPosition(terminationPositionDecoder.logPosition());
                break;

            case TerminationAckDecoder.TEMPLATE_ID:
                terminationAckDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onTerminationAck(
                    terminationAckDecoder.logPosition(), terminationAckDecoder.memberId());
                break;
        }
    }
}

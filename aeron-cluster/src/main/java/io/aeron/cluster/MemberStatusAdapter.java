/*
 * Copyright 2014-2018 Real Logic Ltd.
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
    private final RecoveryPlanQueryDecoder recoveryPlanQueryDecoder = new RecoveryPlanQueryDecoder();
    private final RecoveryPlanDecoder recoveryPlanDecoder = new RecoveryPlanDecoder();
    private final RecordingLogQueryDecoder recordingLogQueryDecoder = new RecordingLogQueryDecoder();
    private final RecordingLogDecoder recordingLogDecoder = new RecordingLogDecoder();
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

    @SuppressWarnings("MethodLength")
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

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
                    canvassPositionDecoder.logPosition(),
                    canvassPositionDecoder.leadershipTermId(),
                    canvassPositionDecoder.followerMemberId());
                break;

            case RequestVoteDecoder.TEMPLATE_ID:
                requestVoteDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onRequestVote(
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
                    newLeadershipTermDecoder.logPosition(),
                    newLeadershipTermDecoder.leadershipTermId(),
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
                    appendedPositionDecoder.logPosition(),
                    appendedPositionDecoder.leadershipTermId(),
                    appendedPositionDecoder.followerMemberId());
                break;

            case CommitPositionDecoder.TEMPLATE_ID:
                commitPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onCommitPosition(
                    commitPositionDecoder.logPosition(),
                    commitPositionDecoder.leadershipTermId(),
                    commitPositionDecoder.leaderMemberId());
                break;

            case RecoveryPlanQueryDecoder.TEMPLATE_ID:
                recoveryPlanQueryDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onRecoveryPlanQuery(
                    recoveryPlanQueryDecoder.correlationId(),
                    recoveryPlanQueryDecoder.leaderMemberId(),
                    recoveryPlanQueryDecoder.requestMemberId());
                break;

            case RecoveryPlanDecoder.TEMPLATE_ID:
                recoveryPlanDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onRecoveryPlan(recoveryPlanDecoder);
                break;

            case RecordingLogQueryDecoder.TEMPLATE_ID:
                recordingLogQueryDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onRecordingLogQuery(
                    recordingLogQueryDecoder.correlationId(),
                    recordingLogQueryDecoder.leaderMemberId(),
                    recordingLogQueryDecoder.requestMemberId(),
                    recordingLogQueryDecoder.fromLeadershipTermId(),
                    recordingLogQueryDecoder.count(),
                    recordingLogQueryDecoder.includeSnapshots() == BooleanType.TRUE);
                break;

            case RecordingLogDecoder.TEMPLATE_ID:
                recordingLogDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onRecordingLog(recordingLogDecoder);
                break;

            default:
                throw new IllegalStateException("unknown template id: " + templateId);
        }
    }
}

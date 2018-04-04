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
    private final RequestVoteDecoder requestVoteDecoder = new RequestVoteDecoder();
    private final VoteDecoder voteDecoder = new VoteDecoder();
    private final AppendedPositionDecoder appendedPositionDecoder = new AppendedPositionDecoder();
    private final CommitPositionDecoder commitPositionDecoder = new CommitPositionDecoder();
    private final QueryResponseDecoder queryResponseDecoder = new QueryResponseDecoder();
    private final RecoveryPlanQueryDecoder recoveryPlanQueryDecoder = new RecoveryPlanQueryDecoder();

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

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case RequestVoteDecoder.TEMPLATE_ID:
                requestVoteDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onRequestVote(
                    requestVoteDecoder.candidateTermId(),
                    requestVoteDecoder.lastBaseLogPosition(),
                    requestVoteDecoder.lastTermPosition(),
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

            case AppendedPositionDecoder.TEMPLATE_ID:
                appendedPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                memberStatusListener.onAppendedPosition(
                    appendedPositionDecoder.termPosition(),
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
                    commitPositionDecoder.termPosition(),
                    commitPositionDecoder.leadershipTermId(),
                    commitPositionDecoder.leaderMemberId(),
                    commitPositionDecoder.logSessionId());
                break;

            case QueryResponseDecoder.TEMPLATE_ID:
                queryResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final int dataOffset = offset +
                    MessageHeaderDecoder.ENCODED_LENGTH +
                    QueryResponseDecoder.BLOCK_LENGTH +
                    QueryResponseDecoder.encodedResponseHeaderLength();

                memberStatusListener.onQueryResponse(
                    queryResponseDecoder.correlationId(),
                    queryResponseDecoder.requestMemberId(),
                    queryResponseDecoder.responseMemberId(),
                    buffer,
                    dataOffset,
                    queryResponseDecoder.encodedResponseLength());
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

            default:
                throw new IllegalStateException("Unknown template id: " + templateId);
        }
    }
}

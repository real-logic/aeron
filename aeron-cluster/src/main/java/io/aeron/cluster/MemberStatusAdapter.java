/*
 * Copyright 2017 Real Logic Ltd.
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
    private final NewLeadershipTermDecoder newLeadershipTermDecoder = new NewLeadershipTermDecoder();
    private final CommittedPositionDecoder committedPositionDecoder = new CommittedPositionDecoder();
    private final AppendedPositionDecoder appendedPositionDecoder = new AppendedPositionDecoder();
    private final QuorumPositionDecoder quorumPositionDecoder = new QuorumPositionDecoder();

    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this);
    private final Subscription subscription;
    private final SequencerAgent sequencerAgent;

    MemberStatusAdapter(final Subscription subscription, final SequencerAgent sequencerAgent)
    {
        this.subscription = subscription;
        this.sequencerAgent = sequencerAgent;
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
            case NewLeadershipTermDecoder.TEMPLATE_ID:
                newLeadershipTermDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onNewLeadershipTerm(
                    newLeadershipTermDecoder.leadershipTermId(),
                    newLeadershipTermDecoder.lastTermPosition(),
                    newLeadershipTermDecoder.logPosition(),
                    newLeadershipTermDecoder.leaderMemberId(),
                    newLeadershipTermDecoder.logSessionId());
                break;

            case CommittedPositionDecoder.TEMPLATE_ID:
                committedPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onCommittedPosition(
                    committedPositionDecoder.termPosition(),
                    committedPositionDecoder.leadershipTermId(),
                    committedPositionDecoder.memberId());
                break;

            case AppendedPositionDecoder.TEMPLATE_ID:
                appendedPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onAppendedPosition(
                    appendedPositionDecoder.termPosition(),
                    appendedPositionDecoder.leadershipTermId(),
                    appendedPositionDecoder.memberId());
                break;

            case QuorumPositionDecoder.TEMPLATE_ID:
                quorumPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onQuorumPosition(
                    quorumPositionDecoder.termPosition(),
                    quorumPositionDecoder.leadershipTermId(),
                    quorumPositionDecoder.leaderMemberId());
                break;

            default:
                throw new IllegalStateException("Unknown template id: " + templateId);
        }
    }
}

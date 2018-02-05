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
    private final AppliedPositionDecoder appliedPositionDecoder = new AppliedPositionDecoder();
    private final AppendedPositionDecoder appendedPositionDecoder = new AppendedPositionDecoder();
    private final CommitPositionDecoder commitPositionDecoder = new CommitPositionDecoder();

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
            case AppliedPositionDecoder.TEMPLATE_ID:
                appliedPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onAppliedPosition(
                    appliedPositionDecoder.termPosition(),
                    appliedPositionDecoder.leadershipTermId(),
                    appliedPositionDecoder.memberId());
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

            case CommitPositionDecoder.TEMPLATE_ID:
                commitPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onCommitPosition(
                    commitPositionDecoder.termPosition(),
                    commitPositionDecoder.leadershipTermId(),
                    commitPositionDecoder.leaderMemberId(),
                    commitPositionDecoder.logSessionId());
                break;

            default:
                throw new IllegalStateException("Unknown template id: " + templateId);
        }
    }
}

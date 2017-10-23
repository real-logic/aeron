/*
 *  Copyright 2017 Real Logic Ltd.
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
package io.aeron.cluster.client;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.NewLeaderSessionEventDecoder;
import io.aeron.cluster.codecs.SessionEventDecoder;
import io.aeron.cluster.codecs.SessionHeaderDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

public class ClusterEventAdapter implements FragmentHandler
{
    /**
     * Length of the session header that will be prepended to the message.
     */
    public static final int SESSION_HEADER_LENGTH =
        MessageHeaderDecoder.ENCODED_LENGTH + SessionHeaderDecoder.BLOCK_LENGTH;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionEventDecoder sessionEventDecoder = new SessionEventDecoder();
    private final NewLeaderSessionEventDecoder newLeaderSessionEventDecoder = new NewLeaderSessionEventDecoder();
    private final SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this);
    private final ClusterEventListener listener;
    private final Subscription subscription;
    private final int fragmentLimit;

    public ClusterEventAdapter(
        final ClusterEventListener listener, final Subscription subscription, final int fragmentLimit)
    {
        this.listener = listener;
        this.subscription = subscription;
        this.fragmentLimit = fragmentLimit;
    }

    public int poll()
    {
        return subscription.poll(fragmentAssembler, fragmentLimit);
    }

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case SessionEventDecoder.TEMPLATE_ID:
                sessionEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                listener.sessionEvent(
                    sessionEventDecoder.correlationId(),
                    sessionEventDecoder.clusterSessionId(),
                    sessionEventDecoder.code(),
                    sessionEventDecoder.detail());
                break;

            case NewLeaderSessionEventDecoder.TEMPLATE_ID:
                newLeaderSessionEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                listener.newLeader(
                    newLeaderSessionEventDecoder.lastCorrelationId(),
                    newLeaderSessionEventDecoder.clusterSessionId(),
                    newLeaderSessionEventDecoder.lastMessageTimestamp(),
                    newLeaderSessionEventDecoder.clusterTermTimestamp(),
                    newLeaderSessionEventDecoder.clusterMessageIndex(),
                    newLeaderSessionEventDecoder.clusterTermId(),
                    newLeaderSessionEventDecoder.clusterLeader());
                break;

            case SessionHeaderDecoder.TEMPLATE_ID:
                sessionHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                listener.onMessage(
                    sessionHeaderDecoder.correlationId(),
                    sessionHeaderDecoder.clusterSessionId(),
                    sessionHeaderDecoder.timestamp(),
                    buffer,
                    offset + SESSION_HEADER_LENGTH,
                    length - SESSION_HEADER_LENGTH,
                    header);
                break;

            default:
                throw new IllegalStateException("Unknown templateId: " + templateId);
        }
    }
}

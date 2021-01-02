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
package io.aeron.cluster.service;

import io.aeron.BufferBuilder;
import io.aeron.Image;
import io.aeron.cluster.client.*;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.*;
import org.agrona.*;

import static io.aeron.logbuffer.FrameDescriptor.*;

/**
 * Adapter for reading a log with a upper bound applied beyond which the consumer cannot progress.
 */
final class BoundedLogAdapter implements ControlledFragmentHandler, AutoCloseable
{
    private final int fragmentLimit;
    private long maxLogPosition;
    private Image image;
    private final ClusteredServiceAgent agent;
    private final BufferBuilder builder = new BufferBuilder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionHeaderDecoder = new SessionMessageHeaderDecoder();
    private final TimerEventDecoder timerEventDecoder = new TimerEventDecoder();
    private final SessionOpenEventDecoder openEventDecoder = new SessionOpenEventDecoder();
    private final SessionCloseEventDecoder closeEventDecoder = new SessionCloseEventDecoder();
    private final ClusterActionRequestDecoder actionRequestDecoder = new ClusterActionRequestDecoder();
    private final NewLeadershipTermEventDecoder newLeadershipTermEventDecoder = new NewLeadershipTermEventDecoder();
    private final MembershipChangeEventDecoder membershipChangeEventDecoder = new MembershipChangeEventDecoder();

    BoundedLogAdapter(final ClusteredServiceAgent agent, final int fragmentLimit)
    {
        this.agent = agent;
        this.fragmentLimit = fragmentLimit;
    }

    public void close()
    {
        if (null != image)
        {
            CloseHelper.close(image.subscription());
            image = null;
        }
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        Action action = Action.CONTINUE;
        final byte flags = header.flags();

        if ((flags & UNFRAGMENTED) == UNFRAGMENTED)
        {
            action = onMessage(buffer, offset, length, header);
        }
        else
        {
            if ((flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG)
            {
                builder.reset().append(buffer, offset, length);
            }
            else
            {
                final int limit = builder.limit();
                if (limit > 0)
                {
                    builder.append(buffer, offset, length);

                    if ((flags & END_FRAG_FLAG) == END_FRAG_FLAG)
                    {
                        action = onMessage(builder.buffer(), 0, builder.limit(), header);

                        if (Action.ABORT == action)
                        {
                            builder.limit(limit);
                        }
                        else
                        {
                            builder.reset();
                        }
                    }
                }
            }
        }

        return action;
    }

    void maxLogPosition(final long position)
    {
        maxLogPosition = position;
    }

    boolean isDone()
    {
        return image.position() >= maxLogPosition || image.isEndOfStream() || image.isClosed();
    }

    void image(final Image image)
    {
        this.image = image;
    }

    Image image()
    {
        return image;
    }

    int poll(final long limit)
    {
        return image.boundedControlledPoll(this, limit, fragmentLimit);
    }

    @SuppressWarnings("MethodLength")
    private Action onMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        final int templateId = messageHeaderDecoder.templateId();
        if (templateId == SessionMessageHeaderDecoder.TEMPLATE_ID)
        {
            sessionHeaderDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            agent.onSessionMessage(
                header.position(),
                sessionHeaderDecoder.clusterSessionId(),
                sessionHeaderDecoder.timestamp(),
                buffer,
                offset + AeronCluster.SESSION_HEADER_LENGTH,
                length - AeronCluster.SESSION_HEADER_LENGTH,
                header);

            return Action.CONTINUE;
        }

        switch (templateId)
        {
            case TimerEventDecoder.TEMPLATE_ID:
                timerEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                agent.onTimerEvent(
                    header.position(),
                    timerEventDecoder.correlationId(),
                    timerEventDecoder.timestamp());
                break;

            case SessionOpenEventDecoder.TEMPLATE_ID:
                openEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final String responseChannel = openEventDecoder.responseChannel();
                final byte[] encodedPrincipal = new byte[openEventDecoder.encodedPrincipalLength()];
                openEventDecoder.getEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.length);

                agent.onSessionOpen(
                    openEventDecoder.leadershipTermId(),
                    header.position(),
                    openEventDecoder.clusterSessionId(),
                    openEventDecoder.timestamp(),
                    openEventDecoder.responseStreamId(),
                    responseChannel,
                    encodedPrincipal);
                break;

            case SessionCloseEventDecoder.TEMPLATE_ID:
                closeEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                agent.onSessionClose(
                    closeEventDecoder.leadershipTermId(),
                    header.position(),
                    closeEventDecoder.clusterSessionId(),
                    closeEventDecoder.timestamp(),
                    closeEventDecoder.closeReason());
                break;

            case ClusterActionRequestDecoder.TEMPLATE_ID:
                actionRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                agent.onServiceAction(
                    actionRequestDecoder.leadershipTermId(),
                    actionRequestDecoder.logPosition(),
                    actionRequestDecoder.timestamp(),
                    actionRequestDecoder.action());
                break;

            case NewLeadershipTermEventDecoder.TEMPLATE_ID:
                newLeadershipTermEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                agent.onNewLeadershipTermEvent(
                    newLeadershipTermEventDecoder.leadershipTermId(),
                    newLeadershipTermEventDecoder.logPosition(),
                    newLeadershipTermEventDecoder.timestamp(),
                    newLeadershipTermEventDecoder.termBaseLogPosition(),
                    newLeadershipTermEventDecoder.leaderMemberId(),
                    newLeadershipTermEventDecoder.logSessionId(),
                    ClusterClock.map(newLeadershipTermEventDecoder.timeUnit()),
                    newLeadershipTermEventDecoder.appVersion());
                break;

            case MembershipChangeEventDecoder.TEMPLATE_ID:
                membershipChangeEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                agent.onMembershipChange(
                    membershipChangeEventDecoder.logPosition(),
                    membershipChangeEventDecoder.timestamp(),
                    membershipChangeEventDecoder.changeType(),
                    membershipChangeEventDecoder.memberId());
                break;
        }

        return Action.CONTINUE;
    }
}

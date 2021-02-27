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

import io.aeron.*;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.*;

import static io.aeron.logbuffer.FrameDescriptor.*;

final class LogAdapter implements ControlledFragmentHandler
{
    private final int fragmentLimit;
    private long logPosition;
    private Image image;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final BufferBuilder builder = new BufferBuilder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionOpenEventDecoder sessionOpenEventDecoder = new SessionOpenEventDecoder();
    private final SessionCloseEventDecoder sessionCloseEventDecoder = new SessionCloseEventDecoder();
    private final SessionMessageHeaderDecoder sessionHeaderDecoder = new SessionMessageHeaderDecoder();
    private final TimerEventDecoder timerEventDecoder = new TimerEventDecoder();
    private final ClusterActionRequestDecoder clusterActionRequestDecoder = new ClusterActionRequestDecoder();
    private final NewLeadershipTermEventDecoder newLeadershipTermEventDecoder = new NewLeadershipTermEventDecoder();
    private final MembershipChangeEventDecoder membershipChangeEventDecoder = new MembershipChangeEventDecoder();

    LogAdapter(final ConsensusModuleAgent consensusModuleAgent, final int fragmentLimit)
    {
        this.consensusModuleAgent = consensusModuleAgent;
        this.fragmentLimit = fragmentLimit;
    }

    void disconnect(final ErrorHandler errorHandler)
    {
        if (null != image)
        {
            logPosition = image.position();
            CloseHelper.close(errorHandler, image.subscription());
            image = null;
        }
    }

    void disconnect(final ErrorHandler errorHandler, final long maxLogPosition)
    {
        disconnect(errorHandler);
        logPosition = Math.min(logPosition, maxLogPosition);
    }

    ConsensusModuleAgent consensusModuleAgent()
    {
        return consensusModuleAgent;
    }

    long position()
    {
        if (null == image)
        {
            return logPosition;
        }

        return image.position();
    }

    int poll(final long boundPosition)
    {
        return image.boundedControlledPoll(this, boundPosition, fragmentLimit);
    }

    boolean isImageClosed()
    {
        return image.isClosed();
    }

    Image image()
    {
        return image;
    }

    void image(final Image image)
    {
        if (null != this.image)
        {
            logPosition = this.image.position();
        }

        this.image = image;
    }

    void removeDestination(final String destination)
    {
        if (null != image)
        {
            image.subscription().removeDestination(destination);
        }
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        Action action = Action.CONTINUE;
        final byte flags = header.flags();

        if ((flags & UNFRAGMENTED) == UNFRAGMENTED)
        {
            action = onMessage(buffer, offset, header);
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
                        action = onMessage(builder.buffer(), 0, header);

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

    @SuppressWarnings("MethodLength")
    private Action onMessage(final DirectBuffer buffer, final int offset, final Header header)
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

            consensusModuleAgent.onReplaySessionMessage(
                sessionHeaderDecoder.clusterSessionId(),
                sessionHeaderDecoder.timestamp());

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

                consensusModuleAgent.onReplayTimerEvent(
                    timerEventDecoder.correlationId());
                break;

            case SessionOpenEventDecoder.TEMPLATE_ID:
                sessionOpenEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onReplaySessionOpen(
                    header.position(),
                    sessionOpenEventDecoder.correlationId(),
                    sessionOpenEventDecoder.clusterSessionId(),
                    sessionOpenEventDecoder.timestamp(),
                    sessionOpenEventDecoder.responseStreamId(),
                    sessionOpenEventDecoder.responseChannel());
                break;

            case SessionCloseEventDecoder.TEMPLATE_ID:
                sessionCloseEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onReplaySessionClose(
                    sessionCloseEventDecoder.clusterSessionId(),
                    sessionCloseEventDecoder.closeReason());
                break;

            case NewLeadershipTermEventDecoder.TEMPLATE_ID:
                newLeadershipTermEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onReplayNewLeadershipTermEvent(
                    newLeadershipTermEventDecoder.leadershipTermId(),
                    newLeadershipTermEventDecoder.logPosition(),
                    newLeadershipTermEventDecoder.timestamp(),
                    newLeadershipTermEventDecoder.termBaseLogPosition(),
                    ClusterClock.map(newLeadershipTermEventDecoder.timeUnit()),
                    newLeadershipTermEventDecoder.appVersion());
                break;

            case MembershipChangeEventDecoder.TEMPLATE_ID:
                membershipChangeEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onReplayMembershipChange(
                    membershipChangeEventDecoder.leadershipTermId(),
                    membershipChangeEventDecoder.logPosition(),
                    membershipChangeEventDecoder.leaderMemberId(),
                    membershipChangeEventDecoder.changeType(),
                    membershipChangeEventDecoder.memberId(),
                    membershipChangeEventDecoder.clusterMembers());
                return Action.BREAK;

            case ClusterActionRequestDecoder.TEMPLATE_ID:
                clusterActionRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onReplayClusterAction(
                    clusterActionRequestDecoder.leadershipTermId(),
                    clusterActionRequestDecoder.action());
                return Action.BREAK;
        }

        return Action.CONTINUE;
    }
}

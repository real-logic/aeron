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
package io.aeron.cluster.service;

import io.aeron.Image;
import io.aeron.ImageControlledFragmentAssembler;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.CountersReader;

/**
 * Adapter for reading a log with a upper bound applied beyond which the consumer cannot progress.
 */
final class BoundedLogAdapter implements ControlledFragmentHandler, AutoCloseable
{
    private static final int FRAGMENT_LIMIT = 10;
    private static final int INITIAL_BUFFER_LENGTH = 4096;

    private final ImageControlledFragmentAssembler fragmentAssembler = new ImageControlledFragmentAssembler(
        this, INITIAL_BUFFER_LENGTH, true);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionOpenEventDecoder openEventDecoder = new SessionOpenEventDecoder();
    private final SessionCloseEventDecoder closeEventDecoder = new SessionCloseEventDecoder();
    private final SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
    private final TimerEventDecoder timerEventDecoder = new TimerEventDecoder();
    private final ClusterActionRequestDecoder actionRequestDecoder = new ClusterActionRequestDecoder();
    private final NewLeadershipTermEventDecoder newLeadershipTermEventDecoder = new NewLeadershipTermEventDecoder();

    private final Image image;
    private final ReadableCounter upperBound;
    private final ClusteredServiceAgent agent;

    BoundedLogAdapter(final Image image, final ReadableCounter upperBound, final ClusteredServiceAgent agent)
    {
        this.image = image;
        this.upperBound = upperBound;
        this.agent = agent;
    }

    public void close()
    {
        CloseHelper.close(image.subscription());
    }

    boolean isImageClosed()
    {
        return image.isClosed();
    }

    public long position()
    {
        return image.position();
    }

    public boolean isConsumed(final CountersReader counters)
    {
        return image.position() >= CommitPos.getMaxLogPosition(counters, upperBound.counterId());
    }

    public int poll()
    {
        return image.boundedControlledPoll(fragmentAssembler, upperBound.get(), FRAGMENT_LIMIT);
    }

    @SuppressWarnings("MethodLength")
    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);
        final int templateId = messageHeaderDecoder.templateId();

        if (templateId == SessionHeaderDecoder.TEMPLATE_ID)
        {
            sessionHeaderDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            agent.onSessionMessage(
                sessionHeaderDecoder.clusterSessionId(),
                sessionHeaderDecoder.correlationId(),
                sessionHeaderDecoder.timestamp(),
                buffer,
                offset + ClientSession.SESSION_HEADER_LENGTH,
                length - ClientSession.SESSION_HEADER_LENGTH,
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

                agent.onTimerEvent(timerEventDecoder.correlationId(), timerEventDecoder.timestamp());
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
                    openEventDecoder.clusterSessionId(),
                    openEventDecoder.correlationId(),
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
                    actionRequestDecoder.logPosition(),
                    actionRequestDecoder.leadershipTermId(),
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
                    newLeadershipTermEventDecoder.leaderMemberId(),
                    newLeadershipTermEventDecoder.logSessionId());
                break;
        }

        return Action.CONTINUE;
    }
}

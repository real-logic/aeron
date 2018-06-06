/*
 *  Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.Image;
import io.aeron.ImageControlledFragmentAssembler;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

final class LogAdapter implements ControlledFragmentHandler, AutoCloseable
{
    /**
     * Length of the session header that will precede application protocol message.
     */
    public static final int SESSION_HEADER_LENGTH =
        MessageHeaderDecoder.ENCODED_LENGTH + SessionHeaderDecoder.BLOCK_LENGTH;

    private static final int FRAGMENT_LIMIT = 10;

    private final ImageControlledFragmentAssembler fragmentAssembler = new ImageControlledFragmentAssembler(this);
    private final Image image;
    private final SequencerAgent sequencerAgent;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionOpenEventDecoder sessionOpenEventDecoder = new SessionOpenEventDecoder();
    private final SessionCloseEventDecoder sessionCloseEventDecoder = new SessionCloseEventDecoder();
    private final SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
    private final TimerEventDecoder timerEventDecoder = new TimerEventDecoder();
    private final ClusterActionRequestDecoder clusterActionRequestDecoder = new ClusterActionRequestDecoder();

    LogAdapter(final Image image, final SequencerAgent sequencerAgent)
    {
        this.image = image;
        this.sequencerAgent = sequencerAgent;
    }

    public void close()
    {
        image.subscription().close();
    }

    long position()
    {
        return image.position();
    }

    int poll(final long boundPosition)
    {
        return image.boundedControlledPoll(fragmentAssembler, boundPosition, FRAGMENT_LIMIT);
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case SessionHeaderDecoder.TEMPLATE_ID:
                sessionHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onReplaySessionMessage(
                    sessionHeaderDecoder.correlationId(),
                    sessionHeaderDecoder.clusterSessionId(),
                    sessionHeaderDecoder.timestamp(),
                    buffer,
                    offset + SESSION_HEADER_LENGTH,
                    length - SESSION_HEADER_LENGTH,
                    header);
                break;

            case TimerEventDecoder.TEMPLATE_ID:
                timerEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onReplayTimerEvent(
                    timerEventDecoder.correlationId(),
                    timerEventDecoder.timestamp());
                break;

            case SessionOpenEventDecoder.TEMPLATE_ID:
                sessionOpenEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onReplaySessionOpen(
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

                sequencerAgent.onReplaySessionClose(
                    sessionCloseEventDecoder.correlationId(),
                    sessionCloseEventDecoder.clusterSessionId(),
                    sessionCloseEventDecoder.timestamp(),
                    sessionCloseEventDecoder.closeReason());
                break;

            case ClusterActionRequestDecoder.TEMPLATE_ID:
                clusterActionRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onReplayClusterAction(
                    clusterActionRequestDecoder.logPosition(),
                    clusterActionRequestDecoder.leadershipTermId(),
                    clusterActionRequestDecoder.timestamp(),
                    clusterActionRequestDecoder.action());
                return Action.BREAK;
        }

        return Action.CONTINUE;
    }
}

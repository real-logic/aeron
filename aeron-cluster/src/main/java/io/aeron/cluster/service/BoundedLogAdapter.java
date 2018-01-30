/*
 * Copyright 2018 Real Logic Ltd.
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
import org.agrona.DirectBuffer;

/**
 * Adapter for reading a log with a limit applied beyond which the consumer cannot progress.
 */
final class BoundedLogAdapter implements ControlledFragmentHandler
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

    private final Image image;
    private final ReadableCounter limit;
    private final ClusteredServiceAgent agent;

    BoundedLogAdapter(final Image image, final ReadableCounter limit, final ClusteredServiceAgent agent)
    {
        this.image = image;
        this.limit = limit;
        this.agent = agent;
    }

    public Image image()
    {
        return image;
    }

    public int poll()
    {
        return image.boundedControlledPoll(fragmentAssembler, limit.get(), FRAGMENT_LIMIT);
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        switch (messageHeaderDecoder.templateId())
        {
            case SessionHeaderDecoder.TEMPLATE_ID:
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

                break;
            }

            case TimerEventDecoder.TEMPLATE_ID:
            {
                timerEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                agent.onTimerEvent(timerEventDecoder.correlationId(), timerEventDecoder.timestamp());
                break;
            }

            case SessionOpenEventDecoder.TEMPLATE_ID:
            {
                openEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final String responseChannel = openEventDecoder.responseChannel();
                final byte[] principalData = new byte[openEventDecoder.principalDataLength()];
                openEventDecoder.getPrincipalData(principalData, 0, principalData.length);

                agent.onSessionOpen(
                    openEventDecoder.clusterSessionId(),
                    openEventDecoder.timestamp(),
                    openEventDecoder.responseStreamId(),
                    responseChannel,
                    principalData);
                break;
            }

            case SessionCloseEventDecoder.TEMPLATE_ID:
            {
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
            }

            case ClusterActionRequestDecoder.TEMPLATE_ID:
            {
                actionRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                agent.onServiceAction(
                    header.position(),
                    actionRequestDecoder.timestamp(),
                    actionRequestDecoder.action());
                break;
            }
        }

        return Action.CONTINUE;
    }
}

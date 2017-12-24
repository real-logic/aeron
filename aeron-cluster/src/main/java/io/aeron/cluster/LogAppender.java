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

import io.aeron.Publication;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

class LogAppender implements AutoCloseable
{
    private static final int SEND_ATTEMPTS = 3;

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final SessionHeaderEncoder sessionHeaderEncoder = new SessionHeaderEncoder();
    private final SessionOpenEventEncoder connectEventEncoder = new SessionOpenEventEncoder();
    private final SessionCloseEventEncoder closeEventEncoder = new SessionCloseEventEncoder();
    private final TimerEventEncoder timerEventEncoder = new TimerEventEncoder();
    private final ServiceActionRequestEncoder actionRequestEncoder = new ServiceActionRequestEncoder();
    private final BufferClaim bufferClaim = new BufferClaim();
    private final Publication publication;

    LogAppender(final Publication publication)
    {
        this.publication = publication;
    }

    public void close()
    {
        CloseHelper.close(publication);
    }

    public long position()
    {
        return publication.position();
    }

    public boolean appendMessage(final DirectBuffer buffer, final int offset, final int length, final long nowMs)
    {
        sessionHeaderEncoder
            .wrap((UnsafeBuffer)buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH)
            .timestamp(nowMs);

        int attempts = SEND_ATTEMPTS;
        do
        {
            if (publication.offer(buffer, offset, length) > 0)
            {
                return true;
            }
        }
        while (--attempts > 0);

        return false;
    }

    public boolean appendConnectedSession(final ClusterSession session, final long timestampMs)
    {
        final String channel = session.responsePublication().channel();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH +
            SessionOpenEventEncoder.BLOCK_LENGTH +
            SessionOpenEventEncoder.responseChannelHeaderLength() +
            channel.length();

        int attempts = SEND_ATTEMPTS;
        do
        {
            if (publication.tryClaim(length, bufferClaim) > 0)
            {
                connectEventEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .clusterSessionId(session.id())
                    .correlationId(session.lastCorrelationId())
                    .timestamp(timestampMs)
                    .responseStreamId(session.responsePublication().streamId())
                    .responseChannel(channel);

                bufferClaim.commit();

                return true;
            }
        }
        while (--attempts > 0);

        return false;
    }

    public boolean appendClosedSession(final ClusterSession session, final CloseReason closeReason, final long nowMs)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseEventEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            if (publication.tryClaim(length, bufferClaim) > 0)
            {
                closeEventEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .clusterSessionId(session.id())
                    .timestamp(nowMs)
                    .closeReason(closeReason);

                bufferClaim.commit();

                return true;
            }
        }
        while (--attempts > 0);

        return false;
    }

    public boolean appendTimerEvent(final long correlationId, final long nowMs)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + TimerEventEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            if (publication.tryClaim(length, bufferClaim) > 0)
            {
                timerEventEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .timestamp(nowMs);

                bufferClaim.commit();

                return true;
            }
        }
        while (--attempts > 0);

        return false;
    }

    public boolean appendActionRequest(
        final ServiceAction action, final long nowMs, final long logPosition, final long messageIndex)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ServiceActionRequestEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            if (publication.tryClaim(length, bufferClaim) > 0)
            {
                actionRequestEncoder.wrapAndApplyHeader(
                    bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .logPosition(logPosition)
                    .messageIndex(messageIndex)
                    .timestamp(nowMs)
                    .action(action);

                bufferClaim.commit();

                return true;
            }
        }
        while (--attempts > 0);

        return false;
    }
}

/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.cluster.client;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;

/**
 * Adapter for dispatching egress messages from a cluster to a {@link EgressListener}.
 */
public final class EgressAdapter implements FragmentHandler
{
    private final long clusterSessionId;
    private final int fragmentLimit;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionEventDecoder sessionEventDecoder = new SessionEventDecoder();
    private final NewLeaderEventDecoder newLeaderEventDecoder = new NewLeaderEventDecoder();
    private final AdminResponseDecoder adminResponseDecoder = new AdminResponseDecoder();
    private final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this);
    private final EgressListener listener;
    private final EgressListenerExtension listenerExtension;
    private final Subscription subscription;

    /**
     * Construct an adapter for cluster egress which consumes from the subscription and dispatches to the
     * {@link EgressListener}.
     *
     * @param listener         to dispatch events to.
     * @param clusterSessionId for the egress.
     * @param subscription     over the egress stream.
     * @param fragmentLimit    to poll on each {@link #poll()} operation.
     */
    public EgressAdapter(
        final EgressListener listener,
        final long clusterSessionId,
        final Subscription subscription,
        final int fragmentLimit)
    {
        this(listener, null, clusterSessionId, subscription, fragmentLimit);
    }

    /**
     * Construct an adapter for cluster egress which consumes from the subscription and dispatches to the
     * {@link EgressListener} or extension messages to {@link EgressListenerExtension}.
     *
     * @param listener          to dispatch events to.
     * @param listenerExtension to dispatch extension messages to
     * @param clusterSessionId  for the egress.
     * @param subscription      over the egress stream.
     * @param fragmentLimit     to poll on each {@link #poll()} operation.
     */
    public EgressAdapter(
        final EgressListener listener,
        final EgressListenerExtension listenerExtension,
        final long clusterSessionId,
        final Subscription subscription,
        final int fragmentLimit)
    {
        this.clusterSessionId = clusterSessionId;
        this.fragmentLimit = fragmentLimit;
        this.listener = listener;
        this.listenerExtension = listenerExtension;
        this.subscription = subscription;
    }

    /**
     * Poll the egress subscription and dispatch assembled events to the {@link EgressListener}.
     *
     * @return the number of fragments consumed.
     */
    public int poll()
    {
        return subscription.poll(fragmentAssembler, fragmentLimit);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("MethodLength")
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            if (listenerExtension != null)
            {
                listenerExtension.onExtensionMessage(
                    messageHeaderDecoder.blockLength(),
                    templateId,
                    schemaId,
                    messageHeaderDecoder.version(),
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    length - MessageHeaderDecoder.ENCODED_LENGTH);
                return;
            }
            throw new ClusterException("expected schemaId=" +
                MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        switch (templateId)
        {
            case SessionMessageHeaderDecoder.TEMPLATE_ID:
            {
                sessionMessageHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = sessionMessageHeaderDecoder.clusterSessionId();
                if (sessionId == clusterSessionId)
                {
                    listener.onMessage(
                        sessionId,
                        sessionMessageHeaderDecoder.timestamp(),
                        buffer,
                        offset + SESSION_HEADER_LENGTH,
                        length - SESSION_HEADER_LENGTH,
                        header);
                }
                break;
            }

            case SessionEventDecoder.TEMPLATE_ID:
            {
                sessionEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = sessionEventDecoder.clusterSessionId();
                if (sessionId == clusterSessionId)
                {
                    listener.onSessionEvent(
                        sessionEventDecoder.correlationId(),
                        sessionId,
                        sessionEventDecoder.leadershipTermId(),
                        sessionEventDecoder.leaderMemberId(),
                        sessionEventDecoder.code(),
                        sessionEventDecoder.detail());
                }
                break;
            }

            case NewLeaderEventDecoder.TEMPLATE_ID:
            {
                newLeaderEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = newLeaderEventDecoder.clusterSessionId();
                if (sessionId == clusterSessionId)
                {
                    listener.onNewLeader(
                        sessionId,
                        newLeaderEventDecoder.leadershipTermId(),
                        newLeaderEventDecoder.leaderMemberId(),
                        newLeaderEventDecoder.ingressEndpoints());
                }
                break;
            }

            case AdminResponseDecoder.TEMPLATE_ID:
            {
                adminResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = adminResponseDecoder.clusterSessionId();
                if (sessionId == clusterSessionId)
                {
                    final long correlationId = adminResponseDecoder.correlationId();
                    final AdminRequestType requestType = adminResponseDecoder.requestType();
                    final AdminResponseCode responseCode = adminResponseDecoder.responseCode();
                    final String message = adminResponseDecoder.message();
                    final int payloadOffset = adminResponseDecoder.offset() +
                        AdminResponseDecoder.BLOCK_LENGTH +
                        AdminResponseDecoder.messageHeaderLength() +
                        message.length() +
                        AdminResponseDecoder.payloadHeaderLength();
                    final int payloadLength = adminResponseDecoder.payloadLength();
                    listener.onAdminResponse(
                        sessionId,
                        correlationId,
                        requestType,
                        responseCode,
                        message,
                        buffer,
                        payloadOffset,
                        payloadLength);
                }
                break;
            }

            default:
                break;
        }
    }
}

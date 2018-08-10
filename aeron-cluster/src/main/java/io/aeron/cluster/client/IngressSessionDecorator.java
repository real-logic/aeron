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
package io.aeron.cluster.client;

import io.aeron.Aeron;
import io.aeron.DirectBufferVector;
import io.aeron.Publication;
import io.aeron.cluster.codecs.IngressMessageHeaderEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Encapsulate applying a client message header for ingress to the cluster.
 * <p>
 * The client message header is applied by a vectored offer to the {@link Publication}.
 * <p>
 * <b>Note:</b> This class is NOT threadsafe. Each publisher thread requires its own instance.
 */
public class IngressSessionDecorator
{
    /**
     * Length of the session header that will be prepended to the message.
     */
    public static final int INGRESS_MESSAGE_HEADER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + IngressMessageHeaderEncoder.BLOCK_LENGTH;

    private long lastCorrelationId;
    private final DirectBufferVector[] vectors = new DirectBufferVector[2];
    private final DirectBufferVector messageVector = new DirectBufferVector();
    private final IngressMessageHeaderEncoder ingressMessageHeaderEncoder = new IngressMessageHeaderEncoder();

    /**
     * Construct a new ingress session header wrapper that defaults all fields to the {@link Aeron#NULL_VALUE}.
     */
    public IngressSessionDecorator()
    {
        this(Aeron.NULL_VALUE, Aeron.NULL_VALUE, Aeron.NULL_VALUE);
    }

    /**
     * Construct a new session header wrapper.
     *
     * @param lastCorrelationId the last correlation id that was sent to the cluster with this session.
     * @param clusterSessionId  that has been allocated by the cluster.
     * @param leadershipTermId  of the current leader.
     */
    public IngressSessionDecorator(
        final long lastCorrelationId, final long clusterSessionId, final long leadershipTermId)
    {
        final UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[INGRESS_MESSAGE_HEADER_LENGTH]);
        ingressMessageHeaderEncoder
            .wrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder())
            .correlationId(lastCorrelationId)
            .clusterSessionId(clusterSessionId)
            .leadershipTermId(leadershipTermId);

        vectors[0] = new DirectBufferVector(headerBuffer, 0, INGRESS_MESSAGE_HEADER_LENGTH);
        vectors[1] = messageVector;

        this.lastCorrelationId = lastCorrelationId;
    }

    /**
     * Reset the cluster session id in the header.
     *
     * @param clusterSessionId to be set in the header.
     * @return this for a fluent API.
     */
    public IngressSessionDecorator clusterSessionId(final long clusterSessionId)
    {
        ingressMessageHeaderEncoder.clusterSessionId(clusterSessionId);
        return this;
    }

    /**
     * Reset the leadership term id in the header.
     *
     * @param leadershipTermId to be set in the header.
     * @return this for a fluent API.
     */
    public IngressSessionDecorator leadershipTermId(final long leadershipTermId)
    {
        ingressMessageHeaderEncoder.leadershipTermId(leadershipTermId);
        return this;
    }

    /**
     * Get the last correlation id generated for this session. Starts with {@link Aeron#NULL_VALUE}.
     *
     * @return the last correlation id generated for this session.
     * @see #nextCorrelationId()
     */
    public long lastCorrelationId()
    {
        return lastCorrelationId;
    }

    /**
     * Generate a new correlation id to be used for this session. This is not threadsafe. If you require a threadsafe
     * correlation id generation then use {@link Aeron#nextCorrelationId()}.
     *
     * @return a new correlation id to be used for this session.
     * @see #lastCorrelationId()
     */
    public long nextCorrelationId()
    {
        return ++lastCorrelationId;
    }

    /**
     * Non-blocking publish of a partial buffer containing a message plus session header to a cluster.
     * <p>
     * This version of the method will set the timestamp value in the header to zero.
     *
     * @param publication   to be offer to.
     * @param correlationId to be used to identify the message to the cluster.
     * @param buffer        containing message.
     * @param offset        offset in the buffer at which the encoded message begins.
     * @param length        in bytes of the encoded message.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)}.
     */
    public long offer(
        final Publication publication,
        final long correlationId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        ingressMessageHeaderEncoder.correlationId(correlationId);
        messageVector.reset(buffer, offset, length);

        return publication.offer(vectors, null);
    }
}

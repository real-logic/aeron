/*
 *  Copyright 2014-2019 Real Logic Ltd.
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
import io.aeron.Publication;
import io.aeron.cluster.codecs.IngressMessageHeaderEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Encapsulate applying a client message header for ingress to the cluster.
 * <p>
 * The client message header is applied to the {@link Publication} before the offered buffer.
 * <p>
 * <b>Note:</b> This class is NOT threadsafe for updating {@link #clusterSessionId(long)} or
 * {@link #leadershipTermId(long)}. Each publisher thread requires its own instance.
 */
public class IngressSessionDecorator
{
    /**
     * Length of the session header that will be prepended to the message.
     */
    public static final int HEADER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + IngressMessageHeaderEncoder.BLOCK_LENGTH;

    private final IngressMessageHeaderEncoder ingressMessageHeaderEncoder = new IngressMessageHeaderEncoder();
    private final UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[HEADER_LENGTH]);

    /**
     * Construct a new ingress session header wrapper that defaults all fields to {@link Aeron#NULL_VALUE}.
     */
    public IngressSessionDecorator()
    {
        this(Aeron.NULL_VALUE, Aeron.NULL_VALUE);
    }

    /**
     * Construct a new session header wrapper.
     *
     * @param clusterSessionId that has been allocated by the cluster.
     * @param leadershipTermId of the current leader.
     */
    public IngressSessionDecorator(final long clusterSessionId, final long leadershipTermId)
    {
        ingressMessageHeaderEncoder
            .wrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder())
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId)
            .timestamp(Aeron.NULL_VALUE);
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
     * Non-blocking publish of a partial buffer containing a message plus session header to a cluster.
     * <p>
     * This version of the method will set the timestamp value in the header to {@link Aeron#NULL_VALUE}.
     *
     * @param publication to be offer to.
     * @param buffer      containing message.
     * @param offset      offset in the buffer at which the encoded message begins.
     * @param length      in bytes of the encoded message.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)}.
     */
    public long offer(final Publication publication, final DirectBuffer buffer, final int offset, final int length)
    {
        return publication.offer(headerBuffer, 0, HEADER_LENGTH, buffer, offset, length, null);
    }
}

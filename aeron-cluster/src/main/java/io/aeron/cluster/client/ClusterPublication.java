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

import io.aeron.DirectBufferVector;
import io.aeron.Publication;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Objects;

/**
 * Encapsulate the {@link Publication} to the cluster and when offering apply the cluster session header.
 * <p>
 * Note: This class is not threadsafe. Each publisher thread requires its own instance.
 */
public class ClusterPublication
{
    /**
     * Length of the session header that will be prepended to the message.
     */
    public static final int SESSION_HEADER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + SessionHeaderEncoder.BLOCK_LENGTH;

    private final Publication publication;
    private final DirectBufferVector[] vectors = new DirectBufferVector[2];
    private final DirectBufferVector messageBuffer = new DirectBufferVector();
    private final SessionHeaderEncoder sessionHeaderEncoder = new SessionHeaderEncoder();

    /**
     * Construct a new cluster {@link Publication} wrapper for a given cluster session id.
     *
     * @param publication      that is connected to the cluster.
     * @param clusterSessionId that has been allocated by the cluster.
     */
    public ClusterPublication(final Publication publication, final long clusterSessionId)
    {
        Objects.requireNonNull(publication);
        this.publication = publication;

        final UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[SESSION_HEADER_LENGTH]);
        sessionHeaderEncoder
            .wrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder())
            .clusterSessionId(clusterSessionId);

        vectors[0] = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
        vectors[1] = messageBuffer;
    }

    /**
     * Non-blocking publish of a partial buffer containing a message to a cluster.
     *
     * @param correlationId to be used to identify the message to the cluster.
     * @param buffer        containing message.
     * @param offset        offset in the buffer at which the encoded message begins.
     * @param length        in bytes of the encoded message.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)}.
     */
    public long offer(final long correlationId, final DirectBuffer buffer, final int offset, final int length)
    {
        sessionHeaderEncoder.correlationId(correlationId);
        messageBuffer.reset(buffer, offset, length);

        return publication.offer(vectors, null);
    }
}

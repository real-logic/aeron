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
package io.aeron.cluster.service;

import io.aeron.DirectBufferVector;
import io.aeron.Publication;
import io.aeron.ReservedValueSupplier;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Session representing a connected client to the cluster.
 */
public class ClientSession
{
    /**
     * Length of the session header that will be prepended to the message.
     */
    public static final int SESSION_HEADER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + SessionHeaderEncoder.BLOCK_LENGTH;

    private final long id;
    private final Publication responsePublication;
    private final DirectBufferVector[] vectors = new DirectBufferVector[2];
    private final DirectBufferVector messageBuffer = new DirectBufferVector();
    private final SessionHeaderEncoder sessionHeaderEncoder = new SessionHeaderEncoder();
    private final Cluster cluster;

    ClientSession(final long sessionId, final Publication responsePublication, final Cluster cluster)
    {
        this.id = sessionId;
        this.responsePublication = responsePublication;
        this.cluster = cluster;

        final UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[SESSION_HEADER_LENGTH]);
        sessionHeaderEncoder
            .wrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder())
            .clusterSessionId(sessionId);

        vectors[0] = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
        vectors[1] = messageBuffer;
    }

    /**
     * Cluster session identity uniquely allocated when the session was opened.
     *
     * @return the cluster session identity uniquely allocated when the session was opened.
     */
    public long id()
    {
        return id;
    }

    /**
     * Non-blocking publish of a partial buffer containing a message to a cluster.
     *
     * @param correlationId to be used to identify the message to the cluster.
     * @param buffer        containing message.
     * @param offset        offset in the buffer at which the encoded message begins.
     * @param length        in bytes of the encoded message.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)} when in {@link Cluster.State#LEADING}
     * otherwise 1.
     */
    public long offer(
        final long correlationId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (cluster.state() != Cluster.State.LEADING)
        {
            return 1;
        }

        sessionHeaderEncoder.correlationId(correlationId);
        sessionHeaderEncoder.timestamp(cluster.timeMs());
        messageBuffer.reset(buffer, offset, length);

        return responsePublication.offer(vectors, null);
    }

    /**
     * Non-blocking publish of a partial buffer containing a message to a cluster.
     *
     * @param correlationId         to be used to identify the message to the cluster.
     * @param buffer                containing message.
     * @param offset                offset in the buffer at which the encoded message begins.
     * @param length                in bytes of the encoded message.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)} when in {@link Cluster.State#LEADING}
     * otherwise 1.
     */
    public long offer(
        final long correlationId,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        if (cluster.state() != Cluster.State.LEADING)
        {
            return 1;
        }

        sessionHeaderEncoder.correlationId(correlationId);
        sessionHeaderEncoder.timestamp(cluster.timeMs());
        messageBuffer.reset(buffer, offset, length);

        return responsePublication.offer(vectors, reservedValueSupplier);
    }

    Publication responsePublication()
    {
        return responsePublication;
    }
}

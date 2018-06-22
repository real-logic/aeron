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

import io.aeron.Aeron;
import io.aeron.DirectBufferVector;
import io.aeron.Publication;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionHeaderEncoder;
import org.agrona.CloseHelper;
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

    /**
     * Return value to indicate egress to a session is mocked out by the cluster when in follower mode.
     */
    public static final long MOCKED_OFFER = 1;

    private final long id;
    private long lastCorrelationId;
    private final int responseStreamId;
    private final String responseChannel;
    private final byte[] encodedPrincipal;
    private final DirectBufferVector[] vectors = new DirectBufferVector[2];
    private final DirectBufferVector messageBuffer = new DirectBufferVector();
    private final SessionHeaderEncoder sessionHeaderEncoder = new SessionHeaderEncoder();
    private final ClusteredServiceAgent cluster;
    private Publication responsePublication;
    private boolean isClosing;

    ClientSession(
        final long sessionId,
        final long correlationId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal,
        final ClusteredServiceAgent cluster)
    {
        this.id = sessionId;
        this.lastCorrelationId = correlationId;
        this.responseStreamId = responseStreamId;
        this.responseChannel = responseChannel;
        this.encodedPrincipal = encodedPrincipal;
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
     * The response channel stream id for responding to the client.
     *
     * @return response channel stream id for responding to the client.
     */
    public int responseStreamId()
    {
        return responseStreamId;
    }

    /**
     * The response channel for responding to the client.
     *
     * @return response channel for responding to the client.
     */
    public String responseChannel()
    {
        return responseChannel;
    }

    /**
     * Cluster session encoded principal from when the session was authenticated.
     *
     * @return The encoded Principal passed. May be 0 length to indicate none present.
     */
    public byte[] encodedPrincipal()
    {
        return encodedPrincipal;
    }

    /**
     * Indicates that a request to close this session has been made.
     *
     * @return whether a request to close this session has been made.
     */
    public boolean isClosing()
    {
        return isClosing;
    }

    /**
     * Get the last correlation id processed on this session.
     *
     * @return the last correlation id processed on this session.
     */
    public long lastCorrelationId()
    {
        return lastCorrelationId;
    }

    /**
     * Non-blocking publish of a partial buffer containing a message to a cluster.
     *
     * @param correlationId to be used to identify the message to the cluster.
     * @param buffer        containing message.
     * @param offset        offset in the buffer at which the encoded message begins.
     * @param length        in bytes of the encoded message.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)} when in {@link Cluster.Role#LEADER}
     * otherwise {@link #MOCKED_OFFER}.
     */
    public long offer(
        final long correlationId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (cluster.role() != Cluster.Role.LEADER)
        {
            return MOCKED_OFFER;
        }

        if (null == responsePublication)
        {
            throw new ClusterException("session not connected id=" + id);
        }

        sessionHeaderEncoder
            .correlationId(correlationId)
            .timestamp(cluster.timeMs());

        messageBuffer.reset(buffer, offset, length);

        return responsePublication.offer(vectors, null);
    }

    /**
     * Non-blocking publish of a partial buffer containing a message to a cluster.
     *
     * @param correlationId to be used to identify the message to the cluster.
     * @param timestampMs   to be used for when the response was generated.
     * @param buffer        containing message.
     * @param offset        offset in the buffer at which the encoded message begins.
     * @param length        in bytes of the encoded message.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)} when in {@link Cluster.Role#LEADER}
     * otherwise {@link #MOCKED_OFFER}.
     */
    public long offer(
        final long correlationId,
        final long timestampMs,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (cluster.role() != Cluster.Role.LEADER)
        {
            return MOCKED_OFFER;
        }

        if (null == responsePublication)
        {
            throw new ClusterException("session not connected id=" + id);
        }

        sessionHeaderEncoder
            .correlationId(correlationId)
            .timestamp(timestampMs);

        messageBuffer.reset(buffer, offset, length);

        return responsePublication.offer(vectors, null);
    }

    void connect(final Aeron aeron)
    {
        if (null == responsePublication)
        {
            responsePublication = aeron.addPublication(responseChannel, responseStreamId);
        }
    }

    void markClosing()
    {
        this.isClosing = true;
    }

    void resetClosing()
    {
        isClosing = false;
    }

    void disconnect()
    {
        CloseHelper.close(responsePublication);
        responsePublication = null;
    }

    void lastCorrelationId(final long correlationId)
    {
        lastCorrelationId = correlationId;
    }
}

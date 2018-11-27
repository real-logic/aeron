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
import io.aeron.Publication;
import io.aeron.exceptions.RegistrationException;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

/**
 * Session representing a connected client to the cluster.
 */
public class ClientSession
{
    /**
     * Return value to indicate egress to a session is mocked out by the cluster when in follower mode.
     */
    public static final long MOCKED_OFFER = 1;

    private final long id;
    private final int responseStreamId;
    private final String responseChannel;
    private final byte[] encodedPrincipal;

    private final ClusteredServiceAgent cluster;
    private Publication responsePublication;
    private boolean isClosing;

    ClientSession(
        final long sessionId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal,
        final ClusteredServiceAgent cluster)
    {
        this.id = sessionId;
        this.responseStreamId = responseStreamId;
        this.responseChannel = responseChannel;
        this.encodedPrincipal = encodedPrincipal;
        this.cluster = cluster;
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
     * Close of this {@link ClientSession} by sending the request to the consensus module.
     * <p>
     * This method is idempotent.
     */
    public void close()
    {
        if (null != cluster.getClientSession(id))
        {
            cluster.closeSession(id);
        }
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
     * Non-blocking publish of a partial buffer containing a message to a cluster.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)} when in {@link Cluster.Role#LEADER}
     * otherwise {@link #MOCKED_OFFER}.
     */
    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        return cluster.offer(id, responsePublication, buffer, offset, length);
    }

    void connect(final Aeron aeron)
    {
        if (null == responsePublication)
        {
            try
            {
                responsePublication = aeron.addExclusivePublication(responseChannel, responseStreamId);
            }
            catch (final RegistrationException ignore)
            {
            }
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
}

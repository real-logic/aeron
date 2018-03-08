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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.CloseHelper;
import org.agrona.collections.ArrayUtil;

import java.util.Arrays;

import static io.aeron.cluster.ClusterSession.Capability.NONE;

class ClusterSession implements AutoCloseable
{
    public static final byte[] NULL_PRINCIPAL = ArrayUtil.EMPTY_BYTE_ARRAY;
    public static final int MAX_ENCODED_PRINCIPAL_LENGTH = 4 * 1024;
    public static final int MAX_ENCODED_ADMIN_RESPONSE_LENGTH = 4 * 1024;

    enum State
    {
        INIT, CONNECTED, CHALLENGED, AUTHENTICATED, REJECTED, OPEN, CLOSED
    }

    /**
     * What a client is capable of doing.
     */
    enum Capability
    {
        /**
         * No capability.
         */
        NONE,

        /**
         * Client can send/receive to/from cluster and can act as a normal client. Can not query endpoints and
         * recording log
         */
        CLIENT_ONLY,

        /**
         * Client can send/receive to/from cluster and can query cluster endpoints and recording log.
         * Normally reserved for cluster members only.
         */
        CLIENT_PLUS_QUERY
    }

    private long timeOfLastActivityMs;
    private long lastCorrelationId;
    private long openedTermPosition = Long.MAX_VALUE;
    private final long id;
    private final int responseStreamId;
    private final String responseChannel;
    private Publication responsePublication;
    private State state = State.INIT;
    private CloseReason closeReason = CloseReason.NULL_VAL;
    private byte[] encodedPrincipal = NULL_PRINCIPAL;
    private byte[] encodedAdminResponse;
    private Capability capability = NONE;

    ClusterSession(final long sessionId, final int responseStreamId, final String responseChannel)
    {
        this.id = sessionId;
        this.responseStreamId = responseStreamId;
        this.responseChannel = responseChannel;
    }

    public void close()
    {
        CloseHelper.close(responsePublication);
        responsePublication = null;
        state = State.CLOSED;
    }

    long id()
    {
        return id;
    }

    int responseStreamId()
    {
        return responseStreamId;
    }

    String responseChannel()
    {
        return responseChannel;
    }

    void closeReason(final CloseReason closeReason)
    {
        this.closeReason = closeReason;
    }

    CloseReason closeReason()
    {
        return closeReason;
    }

    void connect(final Aeron aeron)
    {
        if (null != responsePublication)
        {
            throw new IllegalStateException("Response publication already present");
        }

        responsePublication = aeron.addPublication(responseChannel, responseStreamId);
    }

    Publication responsePublication()
    {
        return responsePublication;
    }

    boolean isResponsePublicationConnected()
    {
        return null != responsePublication && responsePublication.isConnected();
    }

    State state()
    {
        return state;
    }

    void state(final State state)
    {
        this.state = state;
    }

    Capability capability()
    {
        return capability;
    }

    void authenticate(final byte[] encodedPrincipal, final Capability capability)
    {
        if (encodedPrincipal != null)
        {
            this.encodedPrincipal = encodedPrincipal;
        }

        this.state = State.AUTHENTICATED;
        this.capability = capability;
    }

    void open(final long openedTermPosition)
    {
        this.openedTermPosition = openedTermPosition;
        this.state = State.OPEN;
        encodedPrincipal = null;
    }

    byte[] encodedPrincipal()
    {
        return encodedPrincipal;
    }

    void lastActivity(final long timeMs, final long correlationId)
    {
        timeOfLastActivityMs = timeMs;
        lastCorrelationId = correlationId;
    }

    long timeOfLastActivityMs()
    {
        return timeOfLastActivityMs;
    }

    void timeOfLastActivityMs(final long timeMs)
    {
        timeOfLastActivityMs = timeMs;
    }

    long lastCorrelationId()
    {
        return lastCorrelationId;
    }

    long openedTermPosition()
    {
        return openedTermPosition;
    }

    void encodedAdminResponse(final byte[] encodedResponse)
    {
        encodedAdminResponse = encodedResponse;
    }

    byte[] encodedAdminResponse()
    {
        return encodedAdminResponse;
    }

    static void checkEncodedPrincipalLength(final byte[] encodedPrincipal)
    {
        if (null != encodedPrincipal && encodedPrincipal.length > MAX_ENCODED_PRINCIPAL_LENGTH)
        {
            throw new IllegalArgumentException(
                "Encoded Principal max length " +
                MAX_ENCODED_PRINCIPAL_LENGTH +
                " exceeded: length=" +
                encodedPrincipal.length);
        }
    }

    static void checkEncodedAdminResponseLength(final byte[] encodedResponse)
    {
        if (null != encodedResponse &&
            encodedResponse.length > MAX_ENCODED_ADMIN_RESPONSE_LENGTH)
        {
            throw new IllegalArgumentException(
                "Encoded Admin Response max length " +
                    MAX_ENCODED_ADMIN_RESPONSE_LENGTH +
                " exceeded: length=" +
                encodedResponse.length);
        }
    }

    public String toString()
    {
        return "ClusterSession{" +
            "id=" + id +
            ", timeOfLastActivityMs=" + timeOfLastActivityMs +
            ", lastCorrelationId=" + lastCorrelationId +
            ", openedTermPosition=" + openedTermPosition +
            ", responseStreamId=" + responseStreamId +
            ", responseChannel='" + responseChannel + '\'' +
            ", state=" + state +
            ", encodedPrincipal=" + Arrays.toString(encodedPrincipal) +
            ", encodedAdminResponse=" + Arrays.toString(encodedAdminResponse) +
            '}';
    }
}

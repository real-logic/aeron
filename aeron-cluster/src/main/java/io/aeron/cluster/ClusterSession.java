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
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.CloseHelper;
import org.agrona.collections.ArrayUtil;

import java.util.Arrays;

class ClusterSession
{
    static final byte[] NULL_PRINCIPAL = ArrayUtil.EMPTY_BYTE_ARRAY;
    static final int MAX_ENCODED_PRINCIPAL_LENGTH = 4 * 1024;
    static final int MAX_ENCODED_MEMBERSHIP_QUERY_LENGTH = 4 * 1024;

    enum State
    {
        INIT, CONNECTED, CHALLENGED, AUTHENTICATED, REJECTED, OPEN, CLOSED
    }

    private boolean hasNewLeaderEventPending = false;
    private long timeOfLastActivityMs;
    private long lastCorrelationId;
    private long openedLogPosition = Aeron.NULL_VALUE;
    private final long id;
    private final int responseStreamId;
    private final String responseChannel;
    private Publication responsePublication;
    private State state = State.INIT;
    private CloseReason closeReason = CloseReason.NULL_VAL;
    private byte[] encodedPrincipal = NULL_PRINCIPAL;

    ClusterSession(final long sessionId, final int responseStreamId, final String responseChannel)
    {
        this.id = sessionId;
        this.responseStreamId = responseStreamId;
        this.responseChannel = responseChannel;
    }

    ClusterSession(
        final long sessionId,
        final int responseStreamId,
        final String responseChannel,
        final long openedLogPosition,
        final long timeOfLastActivityMs,
        final long lastCorrelationId,
        final CloseReason closeReason)
    {
        this.id = sessionId;
        this.responseStreamId = responseStreamId;
        this.responseChannel = responseChannel;
        this.openedLogPosition = openedLogPosition;
        this.timeOfLastActivityMs = timeOfLastActivityMs;
        this.lastCorrelationId = lastCorrelationId;
        this.closeReason = closeReason;

        if (CloseReason.NULL_VAL != closeReason)
        {
            state = State.CLOSED;
        }
        else
        {
            state = State.OPEN;
        }
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

    void close(final CloseReason closeReason)
    {
        this.closeReason = closeReason;
        close();
    }

    CloseReason closeReason()
    {
        return closeReason;
    }

    void connect(final Aeron aeron)
    {
        if (null != responsePublication)
        {
            throw new ClusterException("response publication already added");
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

    void authenticate(final byte[] encodedPrincipal)
    {
        if (encodedPrincipal != null)
        {
            this.encodedPrincipal = encodedPrincipal;
        }

        this.state = State.AUTHENTICATED;
    }

    void open(final long openedLogPosition)
    {
        this.openedLogPosition = openedLogPosition;
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

    long openedLogPosition()
    {
        return openedLogPosition;
    }

    void hasNewLeaderEventPending(final boolean flag)
    {
        hasNewLeaderEventPending = flag;
    }

    boolean hasNewLeaderEventPending()
    {
        return hasNewLeaderEventPending;
    }

    static void checkEncodedPrincipalLength(final byte[] encodedPrincipal)
    {
        if (null != encodedPrincipal && encodedPrincipal.length > MAX_ENCODED_PRINCIPAL_LENGTH)
        {
            throw new ClusterException(
                "Encoded Principal max length " +
                MAX_ENCODED_PRINCIPAL_LENGTH +
                " exceeded: length=" +
                encodedPrincipal.length);
        }
    }

    public String toString()
    {
        return "ClusterSession{" +
            "id=" + id +
            ", timeOfLastActivityMs=" + timeOfLastActivityMs +
            ", lastCorrelationId=" + lastCorrelationId +
            ", openedLogPosition=" + openedLogPosition +
            ", hasNewLeaderEventPending=" + hasNewLeaderEventPending +
            ", responseStreamId=" + responseStreamId +
            ", responseChannel='" + responseChannel + '\'' +
            ", state=" + state +
            ", closeReason=" + closeReason +
            ", encodedPrincipal=" + Arrays.toString(encodedPrincipal) +
            '}';
    }
}

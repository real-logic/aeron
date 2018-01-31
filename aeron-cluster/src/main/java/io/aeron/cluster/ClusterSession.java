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

import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.CloseHelper;
import org.agrona.collections.ArrayUtil;

import java.util.Arrays;

class ClusterSession implements AutoCloseable
{
    public static final byte[] NULL_PRINCIPAL_DATA = ArrayUtil.EMPTY_BYTE_ARRAY;
    public static final int MAX_PRINCIPAL_DATA_LENGTH = 4 * 1024;

    enum State
    {
        INIT, CONNECTED, CHALLENGED, AUTHENTICATED, REJECTED, OPEN, TIMED_OUT, CLOSED
    }

    private long timeOfLastActivityMs;
    private long lastCorrelationId;
    private long openedTermPosition = Long.MAX_VALUE;
    private final long id;
    private final int responseStreamId;
    private final String responseChannel;
    private Publication responsePublication;
    private State state = State.INIT;
    private byte[] principalData = NULL_PRINCIPAL_DATA;

    ClusterSession(final long sessionId, final int responseStreamId, final String responseChannel)
    {
        this.id = sessionId;
        this.responseStreamId = responseStreamId;
        this.responseChannel = responseChannel;
    }

    public void close()
    {
        CloseHelper.close(responsePublication);
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

    void connect(final Aeron aeron)
    {
        if (null != responsePublication)
        {
            throw new IllegalStateException("Response publication already present");
        }

        responsePublication = aeron.addExclusivePublication(responseChannel, responseStreamId);
    }

    void disconnect()
    {
        CloseHelper.close(responsePublication);
        responsePublication = null;
    }

    Publication responsePublication()
    {
        return responsePublication;
    }

    State state()
    {
        return state;
    }

    void state(final State state)
    {
        this.state = state;
    }

    void authenticate(final byte[] principalData)
    {
        if (principalData != null)
        {
            this.principalData = principalData;
        }

        this.state = State.AUTHENTICATED;
    }

    void open(final long openedTermPosition)
    {
        this.openedTermPosition = openedTermPosition;
        this.state = State.OPEN;
        principalData = null;
    }

    byte[] principalData()
    {
        return principalData;
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

    static void checkPrincipalDataLength(final byte[] principalData)
    {
        if (null != principalData && principalData.length > MAX_PRINCIPAL_DATA_LENGTH)
        {
            throw new IllegalArgumentException(
                "Principal Data max length " +
                    MAX_PRINCIPAL_DATA_LENGTH +
                " exceeded: length=" +
                principalData.length);
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
            ", principalData=" + Arrays.toString(principalData) +
            '}';
    }
}

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

import io.aeron.Publication;
import org.agrona.CloseHelper;

class ClusterSession implements AutoCloseable
{
    enum State
    {
        INIT, CONNECTED, CHALLENGED, AUTHENTICATED, REJECTED, OPEN, TIMED_OUT, CLOSED
    }

    private long timeOfLastActivityMs;
    private long lastCorrelationId;
    private final long id;
    private final Publication responsePublication;
    private State state = State.INIT;

    ClusterSession(final long sessionId, final Publication responsePublication)
    {
        this.id = sessionId;
        this.responsePublication = responsePublication;
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

    public String toString()
    {
        return "ClusterSession{" +
            "id=" + id +
            ", timeOfLastActivityMs=" + timeOfLastActivityMs +
            ", lastCorrelationId=" + lastCorrelationId +
            ", responsePublication=" + responsePublication +
            ", state=" + state +
            '}';
    }
}

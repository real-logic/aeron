/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver;

/**
 * Key used to identify a session instance of a stream.
 */
final class SessionKey
{
    int sessionId;
    final int streamId;
    final String channel;

    SessionKey(final int streamId, final String channel)
    {
        this.streamId = streamId;
        this.channel = channel;
    }

    SessionKey(final int sessionId, final int streamId, final String channel)
    {
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.channel = channel;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final SessionKey that = (SessionKey)o;

        return sessionId == that.sessionId && streamId == that.streamId && channel.equals(that.channel);
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return 31 * sessionId * streamId * channel.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "SessionKey{" +
            "sessionId=" + sessionId +
            ", streamId=" + streamId +
            ", channel=" + channel +
            '}';
    }
}

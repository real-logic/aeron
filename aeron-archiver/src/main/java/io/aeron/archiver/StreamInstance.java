/*
 * Copyright 2014-2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archiver;

import java.util.Objects;

// TODO: maybe remove
class StreamInstance
{
    private final String source;
    private final int sessionId;
    private final String channel;
    private final int streamId;

    StreamInstance(final String source, final int sessionId, final String channel, final int streamId)
    {
        Objects.requireNonNull(source);
        Objects.requireNonNull(channel);

        this.source = source;
        this.sessionId = sessionId;
        this.channel = channel;
        this.streamId = streamId;
    }

    public String source()
    {
        return source;
    }

    public int sessionId()
    {
        return sessionId;
    }

    public String channel()
    {
        return channel;
    }

    public int streamId()
    {
        return streamId;
    }

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

        final StreamInstance that = (StreamInstance) o;

        if (sessionId != that.sessionId)
        {
            return false;
        }
        if (streamId != that.streamId)
        {
            return false;
        }
        if (!source.equals(that.source))
        {
            return false;
        }

        return channel.equals(that.channel);
    }

    public int hashCode()
    {
        return sessionId;
    }
}

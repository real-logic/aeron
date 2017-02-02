/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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

class StreamInstance
{
    private final String source;
    private final int sessionId;
    private final String channel;
    private final int streamId;

    private final String name;
    StreamInstance(String source, int sessionId, String channel, int streamId)
    {
        this.source = source;
        this.sessionId = sessionId;
        this.channel = channel;
        this.streamId = streamId;
        name = ArchiveFileUtil.streamInstanceName(source, sessionId, channel, streamId);
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

    public String name()
    {
        return name;
    }
}

/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import java.nio.ByteBuffer;

/**
 * Aeron Channel
 */
public class Channel
{
    private final Source source;
    private final long channelId;

    public Channel(final Source source, final long channelId)
    {
        this.source = source;
        this.channelId = channelId;
    }

    public long channelId()
    {
        return channelId;
    }

    /**
     * Non blocking message send
     *
     * @param buffer
     * @return
     */
    public boolean offer(final ByteBuffer buffer)
    {
        // TODO
        return false;
    }

    public boolean offer(final ByteBuffer buffer, final int offset)
    {
        // TODO
        return false;
    }

    public void send(final ByteBuffer buffer) throws BufferExhaustedException
    {
        // TODO
    }

    public void send(final ByteBuffer buffer, final int offset) throws BufferExhaustedException
    {
        // TODO
    }

    public void blockingSend(final ByteBuffer buffer)
    {
        // TODO: Is this necessary?
    }

    public void blockingSend(final ByteBuffer buffer, final int offset)
    {
        // TODO: Is this necessary?
    }

}

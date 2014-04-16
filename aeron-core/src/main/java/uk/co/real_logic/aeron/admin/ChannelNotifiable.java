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
package uk.co.real_logic.aeron.admin;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ChannelNotifiable
{

    public static final int BUFFER_COUNT = 3;

    private static final long UNKNOWN_TERM_ID = -1L;

    protected final TermBufferNotifier bufferNotifier;
    protected final String destination;
    protected final long channelId;
    protected final AtomicLong currentTermId;
    protected int currentBuffer;

    public ChannelNotifiable(final TermBufferNotifier bufferNotifier, final String destination, final long channelId)
    {
        this.bufferNotifier = bufferNotifier;
        this.destination = destination;
        this.channelId = channelId;
        currentTermId = new AtomicLong(UNKNOWN_TERM_ID);
        currentBuffer = 0;
    }

    protected boolean hasTerm()
    {
        return currentTermId.get() != UNKNOWN_TERM_ID;
    }

    public void newTermBufferMapped(final long termId, final ByteBuffer buffer)
    {
        bufferNotifier.newTermBufferMapped(termId, buffer);
        if (!hasTerm())
        {
            currentTermId.lazySet(termId);
        }
    }

    protected void startTerm()
    {
        bufferNotifier.termBufferBlocking(currentTermId.get());
    }

    protected void next()
    {
        currentBuffer++;
        if (currentBuffer == BUFFER_COUNT)
        {
            currentBuffer = 0;
        }
        rollTerm();
    }

    protected abstract void rollTerm();

}

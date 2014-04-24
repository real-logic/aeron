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

import uk.co.real_logic.aeron.admin.ChannelNotifiable;
import uk.co.real_logic.aeron.admin.TermBufferNotifier;
import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;

import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.Consumer.MessageFlags.NONE;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.BASE_HEADER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.WORD_ALIGNMENT;

public class ConsumerChannel extends ChannelNotifiable
{
    private static final int HEADER_LENGTH = BitUtil.align(BASE_HEADER_LENGTH, WORD_ALIGNMENT);

    private Long2ObjectHashMap<ConsumerSession> logReaders;
    private final Consumer.DataHandler dataHandler;

    public ConsumerChannel(final Destination destination, final long channelId, final Consumer.DataHandler dataHandler)
    {
        super(new TermBufferNotifier(), destination.destination(), channelId);
        this.dataHandler = dataHandler;
        logReaders = new Long2ObjectHashMap<>();
    }

    public boolean matches(final String destination, final long channelId)
    {
        return this.destination.equals(destination) && this.channelId == channelId;
    }

    public int process() throws Exception
    {
        int count = 0;
        for (final ConsumerSession consumerSession : logReaders.values())
        {
            count += consumerSession.process();
        }
        return count;
    }

    private class ConsumerSession
    {
        private final LogReader[] logReaders;
        private final long sessionId;
        private final AtomicLong currentTermId;
        private final AtomicLong cleanedTermId;

        private int currentBuffer;

        private ConsumerSession(final LogReader[] readers, final long sessionId)
        {
            this.logReaders = readers;
            this.sessionId = sessionId;
            currentTermId = new AtomicLong(UNKNOWN_TERM_ID);
            cleanedTermId = new AtomicLong(UNKNOWN_TERM_ID);
            currentBuffer = 0;
        }

        public int process()
        {
            LogReader logReader = logReaders[currentBuffer];
            if (logReader.isComplete())
            {
                long candidateTermId = currentTermId.get() + 1;
                if (candidateTermId <= cleanedTermId.get())
                {
                    currentBuffer = BitUtil.next(currentBuffer, logReaders.length);
                    logReader = logReaders[currentBuffer];
                    currentTermId.lazySet(candidateTermId);
                }
                else
                {
                    // Need to wait for the next buffer to be cleaned
                    return 0;
                }
            }

            return logReader.read((buffer, offset, length) ->
            {
                dataHandler.onData(buffer, offset + HEADER_LENGTH, sessionId, NONE);
            });
        }

        public void initialTerm(final long termId)
        {
            currentTermId.lazySet(termId);
        }

        public void cleanedTermBuffer(final long termId)
        {
            cleanedTermId.lazySet(termId);
        }

        public boolean hasTerm()
        {
            return currentTermId.get() != UNKNOWN_TERM_ID;
        }
    }

    protected boolean hasTerm(final long sessionId)
    {
        final ConsumerSession consumerSession = logReaders.get(sessionId);
        return consumerSession != null && consumerSession.hasTerm();
    }

    public void initialTerm(final long sessionId, final long termId)
    {
        logReaders.get(sessionId).initialTerm(termId);
    }

    public void cleanedTermBuffer(final long sessionId, final long termId)
    {
        logReaders.get(sessionId).cleanedTermBuffer(termId);
    }

    public void onBuffersMapped(final long sessionId, final LogReader[] logReaders)
    {
        this.logReaders.put(sessionId, new ConsumerSession(logReaders, sessionId));
    }

}

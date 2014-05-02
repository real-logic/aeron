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

import uk.co.real_logic.aeron.conductor.ChannelNotifiable;
import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;

import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.Consumer.MessageFlags.NONE;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.CLEAN_WINDOW;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.rotateId;
import static uk.co.real_logic.aeron.util.ChannelCounters.UNKNOWN_TERM_ID;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.BASE_HEADER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.WORD_ALIGNMENT;

public class ConsumerChannel extends ChannelNotifiable
{
    private static final int HEADER_LENGTH = BitUtil.align(BASE_HEADER_LENGTH, WORD_ALIGNMENT);

    private final Long2ObjectHashMap<ConsumerSession> logReaders = new Long2ObjectHashMap<>();
    private final Consumer.DataHandler dataHandler;

    public ConsumerChannel(final Destination destination, final long channelId, final Consumer.DataHandler dataHandler)
    {
        super(destination.destination(), channelId);

        this.dataHandler = dataHandler;
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

        private int currentBufferId;

        private ConsumerSession(final LogReader[] readers, final long sessionId)
        {
            this.logReaders = readers;
            this.sessionId = sessionId;
            currentTermId = new AtomicLong(UNKNOWN_TERM_ID);
            cleanedTermId = new AtomicLong(UNKNOWN_TERM_ID);
            currentBufferId = 0;
        }

        public int process()
        {
            LogReader logReader = logReaders[currentBufferId];
            if (logReader.isComplete())
            {
                final int candidateBuffer = rotateId(currentBufferId);
                if (currentTermId.get() <= cleanedTermId.get())
                {
                    logReader = logReaders[candidateBuffer];
                    currentBufferId = candidateBuffer;
                    currentTermId.incrementAndGet();
                    logReader.seek(0);
                }
                else
                {
                    // Need to wait for the next buffer to be cleaned
                    return 0;
                }
            }

            return logReader.read(
                    (buffer, offset, length) ->
                    {
                        dataHandler.onData(buffer, offset + HEADER_LENGTH, sessionId, NONE);
                    }
            );
        }

        private boolean hasBeenCleaned(final LogReader logReader)
        {
            return logReader.tailVolatile() == 0;
        }

        public void processBufferScan()
        {
            final long currentTermId = this.currentTermId.get();
            if (currentTermId == UNKNOWN_TERM_ID)
            {
                // Doesn't have any buffers yet
                return;
            }

            final long expectedCleanTermId = currentTermId + CLEAN_WINDOW;
            if (expectedCleanTermId > cleanedTermId.get())
            {
                final int requiredBufferId = rotateId(rotateId((currentBufferId)));
                final LogReader requiredBuffer = logReaders[requiredBufferId];
                if (hasBeenCleaned(requiredBuffer))
                {
                    cleanedTermId.incrementAndGet();
                }
            }
        }

        public void initialTerm(final long termId)
        {
            currentTermId.lazySet(termId);
            cleanedTermId.lazySet(termId + CLEAN_WINDOW);
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

    public void onBuffersMapped(final long sessionId, final LogReader[] logReaders)
    {
        this.logReaders.put(sessionId, new ConsumerSession(logReaders, sessionId));
    }

    public void processBufferScan()
    {
        logReaders.values().forEach(ConsumerSession::processBufferScan);
    }
}

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

import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;

import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.CLEAN_WINDOW;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.rotateId;
import static uk.co.real_logic.aeron.util.ChannelCounters.UNKNOWN_TERM_ID;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.WORD_ALIGNMENT;

/**
 * A subscription that has been connected to from a publisher session.
 */
public class ConnectedSubscription
{
    private static final int HEADER_LENGTH = BitUtil.align(DataHeaderFlyweight.HEADER_LENGTH, WORD_ALIGNMENT);

    private final LogReader[] logReaders;
    private final long sessionId;
    private final Subscription.DataHandler dataHandler;
    private final AtomicLong currentTermId;
    private final AtomicLong cleanedTermId;

    private int currentBufferId = 0;

    public ConnectedSubscription(final LogReader[] readers,
                                 final long sessionId,
                                 final long currentTermId,
                                 final Subscription.DataHandler dataHandler)
    {
        this.logReaders = readers;
        this.sessionId = sessionId;
        this.dataHandler = dataHandler;
        this.currentTermId = new AtomicLong(currentTermId);
        cleanedTermId = new AtomicLong(currentTermId + CLEAN_WINDOW);
    }

    public long sessionId()
    {
        return sessionId;
    }

    public int read()
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

        return logReader.read(Integer.MAX_VALUE, this::onFrame);
    }

    private boolean hasBeenCleaned(final LogReader logReader)
    {
        return logReader.tailVolatile() == 0;
    }

    public int processBufferScan()
    {
        final long currentTermId = this.currentTermId.get();
        if (currentTermId == UNKNOWN_TERM_ID)
        {
            // Doesn't have any buffers yet
            return 0;
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

            return 1;
        }

        return 0;
    }

    private void onFrame(final AtomicBuffer buffer, final int offset, final int length)
    {
        dataHandler.onData(buffer, offset + HEADER_LENGTH, length - HEADER_LENGTH, sessionId);
    }
}

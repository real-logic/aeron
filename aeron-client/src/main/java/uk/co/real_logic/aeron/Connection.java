/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import uk.co.real_logic.aeron.common.concurrent.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.TermReader;
import uk.co.real_logic.agrona.ManagedResource;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.Position;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;

/**
 * Represents an incoming Connection from a publisher to a {@link Subscription}. Each connection identifies source publisher
 * by session id.
 */
class Connection implements ManagedResource
{
    private long timeOfLastStateChange = 0;
    private final long correlationId;
    private final int positionBitsToShift;
    private final int termLengthMask;
    private final int sessionId;

    private final LogBuffers logBuffers;
    private final TermReader[] termReaders = new TermReader[PARTITION_COUNT];

    private final Position subscriberPosition;

    public Connection(
        final int sessionId,
        final long initialPosition,
        final long correlationId,
        final Position subscriberPosition,
        final LogBuffers logBuffers)
    {
        this.correlationId = correlationId;
        this.sessionId = sessionId;
        this.subscriberPosition = subscriberPosition;
        this.logBuffers = logBuffers;

        final UnsafeBuffer[] buffers = logBuffers.atomicBuffers();
        final int initialTermId = initialTermId(buffers[buffers.length - 1]);

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termReaders[i] = new TermReader(initialTermId, buffers[i]);
        }

        final int capacity = termReaders[0].termBuffer().capacity();
        this.termLengthMask = capacity - 1;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(capacity);

        subscriberPosition.setOrdered(initialPosition);
    }

    public int sessionId()
    {
        return sessionId;
    }

    public long correlationId()
    {
        return correlationId;
    }

    public int poll(final FragmentHandler fragmentHandler, final int fragmentCountLimit)
    {
        final long position = subscriberPosition.get();
        final int activeIndex = indexByPosition(position, positionBitsToShift);
        final int termOffset = (int)position & termLengthMask;

        final TermReader termReader = termReaders[activeIndex];
        final int messagesRead = termReader.read(termOffset, fragmentHandler, fragmentCountLimit);

        final long newPosition = position + (termReader.offset() - termOffset);
        if (newPosition > position)
        {
            subscriberPosition.setOrdered(newPosition);
        }

        return messagesRead;
    }

    public void timeOfLastStateChange(final long time)
    {
        this.timeOfLastStateChange = time;
    }

    public long timeOfLastStateChange()
    {
        return timeOfLastStateChange;
    }

    public void delete()
    {
        logBuffers.close();
    }
}

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

import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.ManagedResource;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.Position;

import java.util.Arrays;
import java.util.function.Consumer;

import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.logbuffer.TermReader.*;

/**
 * Represents an incoming Connection from a publisher to a {@link Subscription}. Each connection identifies source publisher
 * by session id.
 */
class Connection implements ManagedResource
{
    private long timeOfLastStateChange = 0;
    private final long correlationId;
    private final int sessionId;
    private final int termLengthMask;
    private final int positionBitsToShift;

    private final Position subscriberPosition;
    private final UnsafeBuffer[] termBuffers;
    private final Header header;
    private final LogBuffers logBuffers;

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
        termBuffers = Arrays.copyOf(buffers, PARTITION_COUNT);

        final int capacity = termBuffers[0].capacity();
        this.termLengthMask = capacity - 1;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(capacity);
        final int initialTermId = initialTermId(buffers[LOG_META_DATA_SECTION_INDEX]);
        header = new Header(initialTermId, capacity);

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

    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit, final Consumer<Throwable> errorHandler)
    {
        final long position = subscriberPosition.get();
        final int termOffset = (int)position & termLengthMask;
        final UnsafeBuffer termBuffer = termBuffers[indexByPosition(position, positionBitsToShift)];

        final long readOutcome = read(termBuffer, termOffset, fragmentHandler, fragmentLimit, header, errorHandler);

        final long newPosition = position + (offset(readOutcome) - termOffset);
        if (newPosition > position)
        {
            subscriberPosition.setOrdered(newPosition);
        }

        return fragmentsRead(readOutcome);
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

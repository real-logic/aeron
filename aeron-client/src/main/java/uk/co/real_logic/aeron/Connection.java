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

import uk.co.real_logic.aeron.common.TermHelper;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogReader;
import uk.co.real_logic.agrona.status.PositionReporter;

import java.util.concurrent.atomic.AtomicInteger;

import static uk.co.real_logic.aeron.common.TermHelper.*;

/**
 * A Connection from a publisher to a subscriber within a given session.
 */
class Connection
{
    private final LogReader[] logReaders;
    private final int sessionId;
    private final long correlationId;
    private final DataHandler dataHandler;
    private final PositionReporter subscriberPosition;
    private final ManagedBuffer[] managedBuffers;
    private final AtomicInteger activeTermId;
    private final int positionBitsToShift;
    private final int initialTermId;

    private int activeIndex;

    public Connection(
        final LogReader[] readers,
        final int sessionId,
        final int initialTermId,
        final long initialPosition,
        final long correlationId,
        final DataHandler dataHandler,
        final PositionReporter subscriberPosition,
        final ManagedBuffer[] managedBuffers)
    {
        this.logReaders = readers;
        this.correlationId = correlationId;
        this.sessionId = sessionId;
        this.dataHandler = dataHandler;
        this.subscriberPosition = subscriberPosition;
        this.managedBuffers = managedBuffers;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(logReaders[0].capacity());
        this.initialTermId = initialTermId;

        final int currentTermId = TermHelper.calculateTermIdFromPosition(initialPosition, positionBitsToShift, initialTermId);
        final int initialTermOffset = TermHelper.calculateTermOffsetFromPosition(initialPosition, positionBitsToShift);
        this.activeTermId = new AtomicInteger(currentTermId);
        this.activeIndex = termIdToBufferIndex(currentTermId);

        logReaders[activeIndex].seek(initialTermOffset);
        subscriberPosition.position(initialPosition);
    }

    public int sessionId()
    {
        return sessionId;
    }

    public long correlationId()
    {
        return correlationId;
    }

    public int poll(final int fragmentCountLimit)
    {
        LogReader logReader = logReaders[activeIndex];
        int activeTermId = this.activeTermId.get();

        if (logReader.isComplete())
        {
            final int nextIndex = rotateNext(activeIndex);
            logReader = logReaders[nextIndex];
            if (logReader.status() != LogBufferDescriptor.CLEAN)
            {
                return 0;
            }

            this.activeTermId.lazySet(++activeTermId);
            this.activeIndex = nextIndex;
            logReader.seek(0);
        }

        final int messagesRead = logReader.read(dataHandler, fragmentCountLimit);
        if (messagesRead > 0)
        {
            final long position = calculatePosition(activeTermId, logReader.offset(), positionBitsToShift, initialTermId);
            subscriberPosition.position(position);
        }

        return messagesRead;
    }

    public void close()
    {
        for (final ManagedBuffer managedBuffer : managedBuffers)
        {
            managedBuffer.close();
        }
    }
}

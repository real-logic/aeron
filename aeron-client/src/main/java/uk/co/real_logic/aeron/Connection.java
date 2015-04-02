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

import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.TermReader;
import uk.co.real_logic.agrona.status.PositionReporter;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;

/**
 * Represents an incoming Connection from a publisher to a {@link Subscription}. Each connection identifies source publisher
 * by session id.
 */
class Connection
{
    private final LogBuffers logBuffers;
    private final TermReader[] termReaders;
    private final DataHandler dataHandler;
    private final PositionReporter subscriberPosition;
    private final long correlationId;
    private final int positionBitsToShift;
    private final int termLengthMask;
    private final int sessionId;

    public Connection(
        final TermReader[] readers,
        final int sessionId,
        final long initialPosition,
        final long correlationId,
        final DataHandler dataHandler,
        final PositionReporter subscriberPosition,
        final LogBuffers logBuffers)
    {
        this.termReaders = readers;
        this.correlationId = correlationId;
        this.sessionId = sessionId;
        this.dataHandler = dataHandler;
        this.subscriberPosition = subscriberPosition;
        this.logBuffers = logBuffers;
        final int capacity = termReaders[0].termBuffer().capacity();
        this.termLengthMask = capacity - 1;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(capacity);

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
        final long position = subscriberPosition.position();
        final int activeIndex = indexByPosition(position, positionBitsToShift);
        final int termOffset = (int)position & termLengthMask;

        final TermReader termReader = termReaders[activeIndex];
        final int messagesRead = termReader.read(termOffset, dataHandler, fragmentCountLimit);

        final long newPosition = position + (termReader.offset() - termOffset);
        subscriberPosition.position(newPosition);

        return messagesRead;
    }

    public void close()
    {
        logBuffers.close();
    }
}

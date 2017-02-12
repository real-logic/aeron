/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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
package io.aeron.driver;

import io.aeron.driver.buffer.RawLog;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.LogBufferUnblocker;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.ReadablePosition;

import static io.aeron.logbuffer.LogBufferDescriptor.*;

/**
 * Encapsulation of a LogBuffer used directly between publishers and subscribers for IPC.
 */
public class IpcPublication implements DriverManagedResource
{
    private static final ReadablePosition[] EMPTY_POSITIONS = new ReadablePosition[0];

    private final long correlationId;
    private final long tripGain;
    private final int sessionId;
    private final int streamId;
    private final int termWindowLength;
    private final int positionBitsToShift;
    private final int initialTermId;
    private long tripLimit = 0;
    private long consumerPosition = 0;
    private long cleanPosition = 0;
    private int refCount = 0;
    private boolean reachedEndOfLife = false;
    private final UnsafeBuffer[] termBuffers;
    private ReadablePosition[] subscriberPositions = EMPTY_POSITIONS;
    private final RawLog rawLog;
    private final Position publisherLimit;

    public IpcPublication(
        final long correlationId,
        final int sessionId,
        final int streamId,
        final Position publisherLimit,
        final RawLog rawLog)
    {
        this.correlationId = correlationId;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.termBuffers = rawLog.termBuffers();
        this.initialTermId = initialTermId(rawLog.metaData());

        final int termLength = rawLog.termLength();
        this.positionBitsToShift = Integer.numberOfTrailingZeros(termLength);
        this.termWindowLength = Configuration.ipcPublicationTermWindowLength(termLength);
        this.tripGain = termWindowLength / 8;
        this.publisherLimit = publisherLimit;
        this.rawLog = rawLog;
    }

    public int sessionId()
    {
        return sessionId;
    }

    public int streamId()
    {
        return streamId;
    }

    public long correlationId()
    {
        return correlationId;
    }

    public RawLog rawLog()
    {
        return rawLog;
    }

    public int publisherLimitId()
    {
        return publisherLimit.id();
    }

    public void close()
    {
        publisherLimit.close();
        for (final ReadablePosition position : subscriberPositions)
        {
            position.close();
        }

        rawLog.close();
    }

    public void addSubscription(final ReadablePosition subscriberPosition)
    {
        subscriberPositions = ArrayUtil.add(subscriberPositions, subscriberPosition);
    }

    public void removeSubscription(final ReadablePosition subscriberPosition)
    {
        subscriberPositions = ArrayUtil.remove(subscriberPositions, subscriberPosition);
        subscriberPosition.close();
    }

    int updatePublishersLimit(final long nowInMillis)
    {
        int workCount = 0;
        long minSubscriberPosition = Long.MAX_VALUE;
        long maxSubscriberPosition = 0;

        for (final ReadablePosition subscriberPosition : subscriberPositions)
        {
            final long position = subscriberPosition.getVolatile();
            minSubscriberPosition = Math.min(minSubscriberPosition, position);
            maxSubscriberPosition = Math.max(maxSubscriberPosition, position);
        }

        if (0 != subscriberPositions.length)
        {
            final long proposedLimit = minSubscriberPosition + termWindowLength;
            if (proposedLimit > tripLimit)
            {
                LogBufferDescriptor.timeOfLastStatusMessage(rawLog.metaData(), nowInMillis);
                publisherLimit.setOrdered(proposedLimit);
                tripLimit = proposedLimit + tripGain;

                cleanBuffer(minSubscriberPosition);

                workCount = 1;
            }

            consumerPosition = maxSubscriberPosition;
        }

        return workCount;
    }

    private void cleanBuffer(final long minConsumerPosition)
    {
        final long cleanPosition = this.cleanPosition;
        final UnsafeBuffer dirtyTerm = termBuffers[indexByPosition(cleanPosition, positionBitsToShift)];
        final int bytesForCleaning = (int)(minConsumerPosition - cleanPosition);
        final int bufferCapacity = dirtyTerm.capacity();
        final int termOffset = (int)cleanPosition & (bufferCapacity - 1);
        final int length = Math.min(bytesForCleaning, bufferCapacity - termOffset);

        if (length > 0)
        {
            dirtyTerm.setMemory(termOffset, length, (byte)0);
            this.cleanPosition = cleanPosition + length;
        }
    }

    public long joiningPosition()
    {
        long maxSubscriberPosition = producerPosition();

        for (final ReadablePosition subscriberPosition : subscriberPositions)
        {
            maxSubscriberPosition = Math.max(maxSubscriberPosition, subscriberPosition.getVolatile());
        }

        return maxSubscriberPosition;
    }

    public long producerPosition()
    {
        final long rawTail = rawTailVolatile(rawLog.metaData());
        final int termOffset = termOffset(rawTail, rawLog.termLength());

        return computePosition(termId(rawTail), termOffset, positionBitsToShift, initialTermId);
    }

    public void onTimeEvent(final long time, final DriverConductor conductor)
    {
        if (0 == refCount)
        {
            reachedEndOfLife = true;
        }
    }

    public boolean hasReachedEndOfLife()
    {
        return reachedEndOfLife;
    }

    public void timeOfLastStateChange(final long time)
    {
        throw new UnsupportedOperationException("not used");
    }

    public long timeOfLastStateChange()
    {
        throw new UnsupportedOperationException("not used");
    }

    public void delete()
    {
        close();
    }

    public int incRef()
    {
        return ++refCount;
    }

    public int decRef()
    {
        return --refCount;
    }

    public long consumerPosition()
    {
        return consumerPosition;
    }

    public boolean unblockAtConsumerPosition()
    {
        return LogBufferUnblocker.unblock(termBuffers, rawLog.metaData(), consumerPosition);
    }
}

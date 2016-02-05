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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.driver.buffer.RawLog;
import uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.logbuffer.LogBufferPartition;
import uk.co.real_logic.aeron.logbuffer.LogBufferUnblocker;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.Position;
import uk.co.real_logic.agrona.concurrent.status.ReadablePosition;

import java.util.ArrayList;
import java.util.List;

import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;

/**
 * Encapsulation of a LogBuffer used directly between publishers and subscribers for IPC.
 */
public class DirectPublication implements DriverManagedResource
{
    private final long correlationId;
    private final long tripGain;
    private long tripLimit = 0;
    private final int sessionId;
    private final int streamId;
    private final int termWindowLength;
    private final int positionBitsToShift;
    private final LogBufferPartition[] logPartitions;
    private final ArrayList<ReadablePosition> subscriberPositions = new ArrayList<>();
    private final RawLog rawLog;

    private final Position publisherLimit;
    private long consumerPosition = 0;
    private int refCount = 0;
    private boolean reachedEndOfLife = false;

    public DirectPublication(
        final long correlationId,
        final int sessionId,
        final int streamId,
        final Position publisherLimit,
        final RawLog rawLog)
    {
        this.correlationId = correlationId;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.logPartitions = rawLog.partitions();

        final int termLength = rawLog.termLength();
        this.termWindowLength = Configuration.ipcPublicationTermWindowLength(termLength);
        this.positionBitsToShift = Integer.numberOfTrailingZeros(termLength);
        this.rawLog = rawLog;
        this.publisherLimit = publisherLimit;
        this.tripGain = this.termWindowLength / 8;

        this.publisherLimit.setOrdered(0);
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
        rawLog.close();
        publisherLimit.close();
        subscriberPositions.forEach(ReadablePosition::close);
    }

    public void addSubscription(final ReadablePosition subscriberPosition)
    {
        subscriberPositions.add(subscriberPosition);
    }

    public void removeSubscription(final ReadablePosition subscriberPosition)
    {
        subscriberPositions.remove(subscriberPosition);
        subscriberPosition.close();
    }

    public int updatePublishersLimit(final long nowInMillis)
    {
        int workCount = 0;
        long minSubscriberPosition = Long.MAX_VALUE;
        long maxSubscriberPosition = 0;

        final List<ReadablePosition> subscriberPositions = this.subscriberPositions;
        for (int i = 0, size = subscriberPositions.size(); i < size; i++)
        {
            final long position = subscriberPositions.get(i).getVolatile();
            minSubscriberPosition = Math.min(minSubscriberPosition, position);
            maxSubscriberPosition = Math.max(maxSubscriberPosition, position);
        }

        long proposedLimit = 0;

        if (!subscriberPositions.isEmpty())
        {
            proposedLimit = minSubscriberPosition + termWindowLength;
            LogBufferDescriptor.timeOfLastSm(rawLog.logMetaData(), nowInMillis);
        }

        if (proposedLimit > tripLimit)
        {
            publisherLimit.setOrdered(proposedLimit);
            tripLimit = proposedLimit + tripGain;
            workCount = 1;
        }

        consumerPosition = maxSubscriberPosition;

        return workCount;
    }

    public int cleanLogBuffer()
    {
        int workCount = 0;

        for (final LogBufferPartition partition : logPartitions)
        {
            if (partition.status() == NEEDS_CLEANING)
            {
                partition.clean();
                workCount = 1;
            }
        }

        return workCount;
    }

    public long joiningPosition()
    {
        long maxSubscriberPosition = producerPosition();

        final List<ReadablePosition> subscriberPositions = this.subscriberPositions;
        for (int i = 0, size = subscriberPositions.size(); i < size; i++)
        {
            final long position = subscriberPositions.get(i).getVolatile();
            maxSubscriberPosition = Math.max(maxSubscriberPosition, position);
        }

        return maxSubscriberPosition;
    }

    public long producerPosition()
    {
        final UnsafeBuffer logMetaDataBuffer = rawLog.logMetaData();
        final int initialTermId = initialTermId(logMetaDataBuffer);
        final long rawTail = logPartitions[activePartitionIndex(logMetaDataBuffer)].rawTailVolatile();
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
        return LogBufferUnblocker.unblock(logPartitions, rawLog.logMetaData(), consumerPosition);
    }
}

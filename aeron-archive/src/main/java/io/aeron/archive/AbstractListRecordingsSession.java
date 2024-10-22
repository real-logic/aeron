/*
 * Copyright 2014-2024 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive;

import io.aeron.Aeron;
import org.agrona.concurrent.UnsafeBuffer;

abstract class AbstractListRecordingsSession implements Session
{
    static final int MAX_SCANS_PER_WORK_CYCLE = 64;

    private final UnsafeBuffer descriptorBuffer;
    private final Catalog catalog;
    private final int count;
    private final ControlSession controlSession;
    private final long correlationId;
    private long recordingId;
    private int sent;
    private boolean isDone = false;

    AbstractListRecordingsSession(
        final long correlationId,
        final long fromRecordingId,
        final int count,
        final Catalog catalog,
        final ControlSession controlSession,
        final UnsafeBuffer descriptorBuffer)
    {
        this.correlationId = correlationId;
        this.recordingId = fromRecordingId;
        this.count = count;
        this.controlSession = controlSession;
        this.catalog = catalog;
        this.descriptorBuffer = descriptorBuffer;
    }

    /**
     * {@inheritDoc}
     */
    public void abort()
    {
        isDone = true;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isDone()
    {
        return isDone;
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return Aeron.NULL_VALUE;
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        if (isDone)
        {
            return 0;
        }

        final CatalogIndex catalogIndex = catalog.index();
        final int lastPosition = catalogIndex.lastPosition();
        final long[] index = catalogIndex.index();
        int position = CatalogIndex.find(index, recordingId, lastPosition);

        if (position < 0)
        {
            for (int i = 0; i <= lastPosition; i += 2)
            {
                if (index[i] >= recordingId)
                {
                    position = i;
                    break;
                }
            }
        }

        final int batchStartPosition = position;
        for (int recordsScanned = 0; sent < count && recordsScanned < MAX_SCANS_PER_WORK_CYCLE; recordsScanned++)
        {
            final boolean noMoreRecordings = position < 0 || position > lastPosition;
            if (noMoreRecordings || catalog.wrapDescriptorAtOffset(descriptorBuffer, (int)index[position + 1]) < 0)
            {
                controlSession.sendRecordingUnknown(correlationId, noMoreRecordings ? recordingId : index[position]);
                isDone = true;
                break;
            }

            if (acceptDescriptor(descriptorBuffer))
            {
                controlSession.sendDescriptor(correlationId, descriptorBuffer);
                ++sent;
            }

            if (position < lastPosition)
            {
                recordingId = index[position + 2];
            }
            else
            {
                recordingId++;
            }
            position += 2;
        }

        if (sent == count)
        {
            isDone = true;
        }

        return (position - batchStartPosition) / 2;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        controlSession.activeListing(null);
    }

    abstract boolean acceptDescriptor(UnsafeBuffer descriptorBuffer);
}

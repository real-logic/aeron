/*
 * Copyright 2014-2020 Real Logic Limited.
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
    static final int MAX_SCANS_PER_WORK_CYCLE = 256;

    final UnsafeBuffer descriptorBuffer;
    final Catalog catalog;
    final ControlSession controlSession;
    final ControlResponseProxy proxy;
    final long correlationId;
    boolean isDone = false;
    private final UnsafeBuffer descriptorBuffer;
    private final Catalog catalog;
    private final int count;
    private final ControlSession controlSession;
    private final ControlResponseProxy proxy;
    private final long correlationId;
    private long recordingId;
    private int sent;
    private boolean isDone = false;

    AbstractListRecordingsSession(
        final long correlationId,
        final long fromRecordingId,
        final int count,
        final Catalog catalog,
        final ControlResponseProxy proxy,
        final ControlSession controlSession,
        final UnsafeBuffer descriptorBuffer)
    {
        this.correlationId = correlationId;
        this.recordingId = fromRecordingId;
        this.count = count;
        this.controlSession = controlSession;
        this.catalog = catalog;
        this.proxy = proxy;
        this.descriptorBuffer = descriptorBuffer;
    }

    public void abort()
    {
        isDone = true;
    }

    public boolean isDone()
    {
        return isDone;
    }

    public long sessionId()
    {
        return Aeron.NULL_VALUE;
    }

    public int doWork()
    {
        if (isDone)
        {
            return 0;
        }

        int totalBytesSent = 0;
        int recordsScanned = 0;

        // FIXME: Use CatalogIndex for the iteration...

        while (sent < count && recordsScanned < MAX_SCANS_PER_WORK_CYCLE)
        {
            if (!catalog.wrapDescriptor(recordingId, descriptorBuffer))
            {
                controlSession.sendRecordingUnknown(correlationId, recordingId, proxy);

                isDone = true;
                break;
            }

            if (Catalog.isValidDescriptor(descriptorBuffer) && acceptDescriptor(descriptorBuffer))
            {
                final int bytesSent = controlSession.sendDescriptor(correlationId, descriptorBuffer, proxy);
                if (bytesSent == 0)
                {
                    isDone = controlSession.isDone();
                    break;
                }

                totalBytesSent += bytesSent;
                ++sent;
            }

            recordingId++;
            recordsScanned++;
        }

        if (sent >= count)
        {
            isDone = true;
        }

        return totalBytesSent;
    }

    public void close()
    {
        controlSession.activeListing(null);
    }

    protected abstract boolean acceptDescriptor(UnsafeBuffer descriptorBuffer);
}

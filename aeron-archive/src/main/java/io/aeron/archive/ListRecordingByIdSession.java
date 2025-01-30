/*
 * Copyright 2014-2025 Real Logic Limited.
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

import org.agrona.concurrent.UnsafeBuffer;

class ListRecordingByIdSession implements Session
{
    private final long correlationId;
    private final long recordingId;
    private final Catalog catalog;
    private final ControlSession controlSession;
    private final UnsafeBuffer descriptorBuffer;
    private boolean isDone;

    ListRecordingByIdSession(
        final long correlationId,
        final long recordingId,
        final Catalog catalog,
        final ControlSession controlSession,
        final UnsafeBuffer descriptorBuffer)
    {
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.catalog = catalog;
        this.controlSession = controlSession;
        this.descriptorBuffer = descriptorBuffer;
    }

    /**
     * {@inheritDoc}
     */
    public void abort(final String reason)
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
    public int doWork()
    {
        if (isDone)
        {
            return 0;
        }

        if (catalog.wrapDescriptor(recordingId, descriptorBuffer))
        {
            if (controlSession.sendDescriptor(correlationId, descriptorBuffer))
            {
                isDone = true;
            }
        }
        else
        {
            controlSession.sendRecordingUnknown(correlationId, recordingId);
            isDone = true;
        }

        return 1;
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return correlationId;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        controlSession.activeListing(null);
    }
}

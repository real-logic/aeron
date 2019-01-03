/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.archive;

import org.agrona.concurrent.UnsafeBuffer;

abstract class AbstractListRecordingsSession implements Session
{
    protected static final int MAX_SCANS_PER_WORK_CYCLE = 256;

    protected final UnsafeBuffer descriptorBuffer;
    protected final Catalog catalog;
    protected final ControlSession controlSession;
    protected final ControlResponseProxy proxy;
    protected final long correlationId;
    protected boolean isDone = false;

    AbstractListRecordingsSession(
        final long correlationId,
        final Catalog catalog,
        final ControlResponseProxy proxy,
        final ControlSession controlSession,
        final UnsafeBuffer descriptorBuffer)
    {
        this.correlationId = correlationId;
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
        return Catalog.NULL_RECORD_ID;
    }

    public int doWork()
    {
        int workCount = 0;

        if (!isDone)
        {
            workCount += sendDescriptors();
        }

        return workCount;
    }

    public void close()
    {
        controlSession.onListRecordingSessionClosed(this);
    }

    protected abstract int sendDescriptors();
}

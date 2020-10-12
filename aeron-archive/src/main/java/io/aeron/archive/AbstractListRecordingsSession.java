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
        return Aeron.NULL_VALUE;
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
        controlSession.activeListing(null);
    }

    abstract int sendDescriptors();
}

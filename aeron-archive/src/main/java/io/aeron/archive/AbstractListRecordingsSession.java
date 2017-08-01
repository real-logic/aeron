/*
 * Copyright 2014-2017 Real Logic Ltd.
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

import io.aeron.Publication;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.archive.Catalog.VALID;
import static io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
import static io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder.SCHEMA_VERSION;

abstract class AbstractListRecordingsSession implements Session
{
    private final RecordingDescriptorHeaderDecoder headerDecoder = new RecordingDescriptorHeaderDecoder();
    private final ControlSession controlSession;
    protected final UnsafeBuffer descriptorBuffer;
    protected final Publication controlPublication;
    protected final Catalog catalog;
    protected final ControlSessionProxy proxy;
    protected final long correlationId;
    protected boolean isDone = false;

    AbstractListRecordingsSession(
        final long correlationId,
        final Publication controlPublication,
        final Catalog catalog,
        final ControlSessionProxy proxy,
        final ControlSession controlSession,
        final UnsafeBuffer descriptorBuffer)
    {
        this.correlationId = correlationId;
        this.controlPublication = controlPublication;
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

    protected boolean isDescriptorValid(final UnsafeBuffer buffer)
    {
        return headerDecoder.wrap(buffer, 0, BLOCK_LENGTH, SCHEMA_VERSION).valid() == VALID;
    }

    protected abstract int sendDescriptors();
}

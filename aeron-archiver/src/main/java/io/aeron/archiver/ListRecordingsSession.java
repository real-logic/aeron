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
package io.aeron.archiver;

import io.aeron.Publication;
import io.aeron.archiver.codecs.ControlResponseCode;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

class ListRecordingsSession implements Session
{
    private enum State
    {
        INIT,
        ACTIVE,
        INACTIVE,
        CLOSED
    }

    private final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(Catalog.RECORD_LENGTH, CACHE_LINE_LENGTH);
    private final UnsafeBuffer descriptorBuffer = new UnsafeBuffer(byteBuffer);

    private final Publication controlPublication;
    private final long fromId;
    private final long toId;
    private final Catalog catalog;
    private final ControlSessionProxy proxy;
    private final long correlationId;

    private long recordingId;
    private State state = State.INIT;

    ListRecordingsSession(
        final long correlationId,
        final Publication controlPublication,
        final long fromId,
        final int count,
        final Catalog catalog,
        final ControlSessionProxy proxy)
    {
        this.controlPublication = controlPublication;
        recordingId = fromId;
        this.fromId = fromId;
        this.toId = fromId + count;
        this.catalog = catalog;
        this.proxy = proxy;
        this.correlationId = correlationId;
    }

    public void abort()
    {
        state = State.INACTIVE;
    }

    public boolean isDone()
    {
        return state == State.INACTIVE;
    }

    public long sessionId()
    {
        return Catalog.NULL_RECORD_ID;
    }

    public int doWork()
    {
        int workDone = 0;

        switch (state)
        {
            case INIT:
                workDone += init();
                break;

            case ACTIVE:
                workDone += sendDescriptors();
                break;
        }

        return workDone;
    }

    public void close()
    {
        state = State.CLOSED;
    }

    private int sendDescriptors()
    {
        int sentBytes = 0;
        do
        {
            final RecordingSession session = catalog.getRecordingSession(recordingId);
            if (session == null)
            {
                byteBuffer.clear();
                descriptorBuffer.wrap(byteBuffer);
                try
                {
                    if (!catalog.readDescriptor(recordingId, byteBuffer))
                    {
                        proxy.sendDescriptorNotFound(
                            correlationId, recordingId, catalog.nextRecordingId(), controlPublication);
                        state = State.INACTIVE;
                        return 0;
                    }
                }
                catch (final IOException ex)
                {
                    state = State.INACTIVE;
                    LangUtil.rethrowUnchecked(ex);
                }
            }
            else
            {
                descriptorBuffer.wrap(session.metaDataBuffer());
            }

            sentBytes += proxy.sendDescriptor(correlationId, descriptorBuffer, controlPublication);

            if (++recordingId >= toId)
            {
                state = State.INACTIVE;
                break;
            }
        }
        while (sentBytes < controlPublication.maxPayloadLength());

        return sentBytes;
    }

    private int init()
    {
        if (fromId >= catalog.nextRecordingId())
        {
            proxy.sendError(
                correlationId,
                ControlResponseCode.RECORDING_NOT_FOUND,
                "Requested start id exceeds max known id",
                controlPublication);

            state = State.INACTIVE;
        }
        else
        {
            state = State.ACTIVE;
        }

        return 1;
    }
}

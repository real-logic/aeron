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

import io.aeron.ExclusivePublication;
import io.aeron.archiver.codecs.ControlResponseCode;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

class ListRecordingsSession implements ArchiveConductor.Session
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

    private final ExclusivePublication reply;
    private final long fromId;
    private final long toId;
    private final Catalog index;
    private final ControlSessionProxy proxy;
    private final long correlationId;

    private long recordingId;
    private State state = State.INIT;

    ListRecordingsSession(
        final long correlationId,
        final ExclusivePublication reply,
        final long fromId,
        final long toId,
        final Catalog index,
        final ControlSessionProxy proxy)
    {
        this.reply = reply;
        recordingId = fromId;
        this.fromId = fromId;
        this.toId = toId;
        this.index = index;
        this.proxy = proxy;
        this.correlationId = correlationId;
    }

    public void abort()
    {
        state = State.INACTIVE;
    }

    public boolean isDone()
    {
        return state == State.CLOSED;
    }

    public void remove(final ArchiveConductor conductor)
    {
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

            case INACTIVE:
                workDone += close();
                break;
        }

        return workDone;
    }

    private int close()
    {
        state = State.CLOSED;
        return 1;
    }

    private int sendDescriptors()
    {
        final RecordingSession session = index.getRecordingSession(recordingId);
        if (session == null)
        {
            byteBuffer.clear();
            descriptorBuffer.wrap(byteBuffer);
            try
            {
                if (!index.readDescriptor(recordingId, byteBuffer))
                {
                    proxy.sendDescriptorNotFound(reply, recordingId, index.maxRecordingId(), correlationId);
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

        proxy.sendDescriptor(reply, descriptorBuffer, correlationId);

        if (++recordingId > toId)
        {
            state = State.INACTIVE;
        }

        return 1;
    }

    private int init()
    {
        if (fromId > toId)
        {
            proxy.sendError(
                reply,
                ControlResponseCode.ERROR,
                "Requested range is reversed (to < from)",
                correlationId);
            state = State.INACTIVE;
        }
        else if (toId > index.maxRecordingId())
        {
            proxy.sendError(
                reply,
                ControlResponseCode.ERROR,
                "Requested range exceeds available range (to > max)",
                correlationId);
            state = State.INACTIVE;
        }
        else
        {
            state = State.ACTIVE;
        }

        return 1;
    }
}

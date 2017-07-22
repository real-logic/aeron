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
import org.agrona.concurrent.UnsafeBuffer;

abstract class AbstractListRecordingsSession implements Session
{
    enum State
    {
        INIT, ACTIVE, INACTIVE, CLOSED
    }

    protected final UnsafeBuffer descriptorBuffer = new UnsafeBuffer();
    protected final Publication controlPublication;
    private final ControlSession controlSession;
    protected final Catalog catalog;
    protected final ControlSessionProxy proxy;
    protected final long correlationId;
    protected State state = State.INIT;

    AbstractListRecordingsSession(
        final long correlationId,
        final Publication controlPublication,
        final Catalog catalog,
        final ControlSessionProxy proxy,
        final ControlSession controlSession)
    {
        this.controlPublication = controlPublication;
        this.controlSession = controlSession;
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

    protected abstract int sendDescriptors();

    protected abstract int init();

    public void close()
    {
        state = State.CLOSED;
        controlSession.onListRecordingSessionClosed(this);
    }

    protected void sendError(final ControlResponseCode code, final String message)
    {
        proxy.sendResponse(correlationId, code, message, controlPublication);
    }
}

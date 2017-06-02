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

import io.aeron.*;
import org.agrona.*;

class ControlSession implements Session, ControlRequestListener
{
    enum State
    {
        INIT, ACTIVE, INACTIVE, CLOSED
    }

    private final Image image;
    private final ControlSessionProxy proxy;
    private final ArchiveConductor conductor;
    private final ControlRequestAdapter adapter = new ControlRequestAdapter(this);
    private Publication controlPublication;
    private State state = State.INIT;

    ControlSession(
        final Image image,
        final ControlSessionProxy clientProxy,
        final ArchiveConductor conductor)
    {
        this.image = image;
        this.proxy = clientProxy;
        this.conductor = conductor;
    }

    public long sessionId()
    {
        return image.correlationId();
    }

    public void abort()
    {
        state = State.INACTIVE;
    }

    public boolean isDone()
    {
        return state == State.CLOSED;
    }

    public int doWork()
    {
        switch (state)
        {
            case INIT:
                return waitForConnection();

            case ACTIVE:
                if (image.isClosed() || !controlPublication.isConnected())
                {
                    state = State.INACTIVE;
                }
                else
                {
                    return image.poll(adapter, 16);
                }
                break;

            case INACTIVE:
                CloseHelper.quietClose(controlPublication);
                state = State.CLOSED;
                break;

            case CLOSED:
                break;

            default:
                throw new IllegalStateException();
        }

        return 0;
    }

    private int waitForConnection()
    {
        if (controlPublication == null)
        {
            try
            {
                image.poll(adapter, 1);
            }
            catch (final Exception ex)
            {
                state = State.INACTIVE;
                LangUtil.rethrowUnchecked(ex);
            }
        }
        else if (controlPublication.isConnected())
        {
            state = State.ACTIVE;
        }
        // TODO: timeout

        return 0;
    }

    public void onConnect(final String channel, final int streamId)
    {
        if (state != State.INIT)
        {
            throw new IllegalStateException();
        }

        controlPublication = conductor.newControlPublication(channel, streamId);
    }

    // TODO: remove external access to record start/stop
    public void onStopRecording(final long correlationId, final long recordingId)
    {
        validateActive();
        conductor.stopRecording(correlationId, controlPublication, recordingId);
    }

    // TODO: remove external access to record start/stop
    public void onStartRecording(final long correlationId, final String channel, final int streamId)
    {
        validateActive();
        conductor.setupRecording(correlationId, controlPublication, channel, streamId);
    }

    public void onListRecordings(final long correlationId, final long fromRecordingId, final int recordCount)
    {
        validateActive();
        conductor.listRecordings(correlationId, controlPublication, fromRecordingId, recordCount);
    }

    public void onAbortReplay(final long correlationId, final long replayId)
    {
        validateActive();
        conductor.stopReplay(correlationId, controlPublication, replayId);
    }

    public void onStartReplay(
        final long correlationId,
        final int replayStreamId,
        final String replayChannel,
        final long recordingId,
        final long position,
        final long length)
    {
        validateActive();

        conductor.startReplay(
            correlationId,
            controlPublication,
            replayStreamId,
            replayChannel,
            recordingId,
            position,
            length);
    }

    private void validateActive()
    {
        if (state != State.ACTIVE)
        {
            throw new IllegalStateException();
        }
    }
}

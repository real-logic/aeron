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

import io.aeron.Image;
import io.aeron.Publication;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochClock;

import java.util.ArrayDeque;

class ControlSession implements Session, ControlRequestListener
{
    enum State
    {
        INIT, ACTIVE, INACTIVE, CLOSED
    }

    private static final long CONTROL_TIMEOUT_MS = 1000L;

    private final Image image;
    private final ArchiveConductor conductor;
    private final EpochClock epochClock;
    private final ControlRequestAdapter adapter = new ControlRequestAdapter(this);
    private Publication controlPublication;
    private State state = State.INIT;
    private long timeConnectedMs;
    private ArrayDeque<ListRecordingsSession> listRecordingsSessions = new ArrayDeque<>();

    ControlSession(final Image image, final ArchiveConductor conductor, final EpochClock epochClock)
    {
        this.image = image;
        this.conductor = conductor;
        this.epochClock = epochClock;
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
        return state == State.INACTIVE;
    }

    public int doWork()
    {
        if (state == State.INIT)
        {
            return waitForConnection();
        }
        else if (state == State.ACTIVE)
        {
            if (image.isClosed() || !controlPublication.isConnected())
            {
                state = State.INACTIVE;
            }
            else
            {
                return image.poll(adapter, 16);
            }
        }

        return 0;
    }

    public void close()
    {
        CloseHelper.quietClose(controlPublication);
        state = State.CLOSED;
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
        else if (timeConnectedMs + CONTROL_TIMEOUT_MS < epochClock.time())
        {
            state = State.INACTIVE;
        }

        return 0;
    }

    public void onConnect(final String channel, final int streamId)
    {
        if (state != State.INIT)
        {
            throw new IllegalStateException();
        }

        controlPublication = conductor.newControlPublication(channel, streamId);
        timeConnectedMs = epochClock.time();
    }

    public void onStopRecording(final long correlationId,
                                final String channel,
                                final int streamId)
    {
        validateActive();
        conductor.stopRecording(correlationId, controlPublication, channel, streamId);
    }

    public void onStartRecording(final long correlationId, final String channel, final int streamId)
    {
        validateActive();
        conductor.startRecordingSubscription(correlationId, controlPublication, channel, streamId);
    }

    public void onListRecordings(final long correlationId, final long fromRecordingId, final int recordCount)
    {
        validateActive();
        final ListRecordingsSession listRecordingsSession = conductor.newListRecordingsSession(
            correlationId,
            controlPublication,
            fromRecordingId,
            recordCount,
            this);
        this.listRecordingsSessions.add(listRecordingsSession);

        if (listRecordingsSessions.size() == 1)
        {
            conductor.addSession(listRecordingsSession);
        }
    }

    void onListRecordingSessionClosed(final ListRecordingsSession listRecordingsSession)
    {
        if (listRecordingsSession != listRecordingsSessions.poll())
        {
            throw new IllegalStateException();
        }

        if (!isDone() && listRecordingsSessions.size() != 0)
        {
            conductor.addSession(listRecordingsSessions.peek());
        }
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

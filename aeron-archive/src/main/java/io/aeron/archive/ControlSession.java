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

import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Publication;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayDeque;

class ControlSession implements Session, ControlRequestListener
{
    private static final int FRAGMENT_LIMIT = 16;

    enum State
    {
        INIT, ACTIVE, INACTIVE, CLOSED
    }

    private static final long CONNECT_TIMEOUT_MS = 5000L;

    private final Image image;
    private final ArchiveConductor conductor;
    private final EpochClock epochClock;
    private final FragmentHandler adapter = new ImageFragmentAssembler(new ControlRequestAdapter(this));
    private Publication controlPublication;
    private State state = State.INIT;
    private long connectDeadlineMs;
    private ArrayDeque<AbstractListRecordingsSession> listRecordingsSessions = new ArrayDeque<>();

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
        int workCount = 0;

        if (state == State.INIT)
        {
            workCount += waitForConnection();
        }

        if (state == State.ACTIVE)
        {
            if (image.isClosed() || !controlPublication.isConnected())
            {
                state = State.INACTIVE;
            }
            else
            {
                workCount += image.poll(adapter, FRAGMENT_LIMIT);
            }
        }

        return workCount;
    }

    public void close()
    {
        state = State.CLOSED;
        CloseHelper.quietClose(controlPublication);
    }

    private int waitForConnection()
    {
        int workCount = 0;

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
            workCount += 1;
        }
        else if (epochClock.time() > connectDeadlineMs)
        {
            state = State.INACTIVE;
        }

        return workCount;
    }

    public void onConnect(final String channel, final int streamId)
    {
        if (state != State.INIT)
        {
            throw new IllegalStateException();
        }

        controlPublication = conductor.newControlPublication(channel, streamId);
        connectDeadlineMs = epochClock.time() + CONNECT_TIMEOUT_MS;
    }

    public void onStopRecording(final long correlationId, final String channel, final int streamId)
    {
        conductor.stopRecording(correlationId, this, channel, streamId);
    }

    public void onStartRecording(
        final long correlationId, final String channel, final int streamId, final SourceLocation sourceLocation)
    {
        conductor.startRecordingSubscription(correlationId, this, channel, streamId, sourceLocation);
    }

    public void onListRecordingsForUri(
        final long correlationId,
        final long fromRecordingId,
        final int recordCount,
        final String channel,
        final int streamId)
    {
        final ListRecordingsForUriSession listRecordingsSession = conductor.newListRecordingsForUriSession(
            correlationId,
            fromRecordingId,
            recordCount,
            conductor.strippedChannelBuilder(channel).build(),
            streamId,
            this);

        listRecordingsSessions.add(listRecordingsSession);

        if (listRecordingsSessions.size() == 1)
        {
            conductor.addSession(listRecordingsSession);
        }
    }

    public void onListRecordings(final long correlationId, final long fromRecordingId, final int recordCount)
    {
        final ListRecordingsSession listRecordingsSession = conductor.newListRecordingsSession(
            correlationId,
            fromRecordingId,
            recordCount,
            this);

        listRecordingsSessions.add(listRecordingsSession);

        if (listRecordingsSessions.size() == 1)
        {
            conductor.addSession(listRecordingsSession);
        }
    }

    public void onStartReplay(
        final long correlationId,
        final int replayStreamId,
        final String replayChannel,
        final long recordingId,
        final long position,
        final long length)
    {
        conductor.startReplay(
            correlationId,
            this,
            replayStreamId,
            replayChannel,
            recordingId,
            position,
            length);
    }

    void onListRecordingSessionClosed(final AbstractListRecordingsSession listRecordingsSession)
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

    boolean sendOkResponse(final long correlationId, final ControlSessionProxy proxy)
    {
        return proxy.sendResponse(
            correlationId,
            0,
            ControlResponseCode.OK,
            null,
            controlPublication);
    }

    boolean sendRecordingUnknown(final long correlationId, final long recordingId, final ControlSessionProxy proxy)
    {
        return proxy.sendResponse(
            correlationId,
            recordingId,
            ControlResponseCode.RECORDING_UNKNOWN,
            null,
            controlPublication);
    }

    boolean sendResponse(
        final long correlationId,
        final ControlResponseCode code,
        final String errorMessage,
        final ControlSessionProxy proxy)
    {
        return proxy.sendResponse(
            correlationId,
            0,
            code,
            errorMessage,
            controlPublication);
    }

    int sendDescriptor(
        final long correlationId,
        final UnsafeBuffer descriptorBuffer,
        final ControlSessionProxy proxy)
    {
        return proxy.sendDescriptor(correlationId, descriptorBuffer, controlPublication);
    }

    int maxPayloadLength()
    {
        return controlPublication.maxPayloadLength();
    }
}

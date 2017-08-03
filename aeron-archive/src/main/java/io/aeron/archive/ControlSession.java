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
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayDeque;
import java.util.function.Supplier;

import static io.aeron.archive.codecs.ControlResponseCode.OK;
import static io.aeron.archive.codecs.ControlResponseCode.RECORDING_UNKNOWN;

/**
 * Control sessions are interacted with from both the {@link ArchiveConductor} and the replay/record
 * {@link SessionWorker}s. The interaction may result in pending send actions being queued for execution by the
 * {@link ArchiveConductor}.
 * This complexity reflects the fact that replay/record/list requests happen in the context of a session, and that they
 * share the sessions request/reply channels. The relationship does not imply a lifecycle dependency however. A
 * {@link RecordingSession}/{@link ReplaySession} can outlive their 'parent' {@link ControlSession}.
 */
class ControlSession implements Session, ControlRequestListener
{
    enum State
    {
        INIT, ACTIVE, INACTIVE, CLOSED
    }

    static final long TIMEOUT_MS = 5000L;
    private static final int FRAGMENT_LIMIT = 16;

    private final Image image;
    private final ArchiveConductor conductor;
    private final EpochClock epochClock;
    private final FragmentHandler adapter = new ImageFragmentAssembler(new ControlRequestAdapter(this));
    private ArrayDeque<AbstractListRecordingsSession> listRecordingsSessions = new ArrayDeque<>();
    private ManyToOneConcurrentLinkedQueue<Supplier<Boolean>> responseQueue = new ManyToOneConcurrentLinkedQueue<>();
    private final ControlResponseProxy controlResponseProxy;
    private Publication controlPublication;
    private State state = State.INIT;
    private long timeoutDeadlineMs = -1;

    ControlSession(
        final Image image,
        final ArchiveConductor conductor,
        final EpochClock epochClock,
        final ControlResponseProxy controlResponseProxy)
    {
        this.image = image;
        this.conductor = conductor;
        this.epochClock = epochClock;
        this.controlResponseProxy = controlResponseProxy;
    }

    public long sessionId()
    {
        return image.correlationId();
    }

    public void abort()
    {
        state = State.INACTIVE;
    }

    public void close()
    {
        state = State.CLOSED;
        CloseHelper.quietClose(controlPublication);
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
            workCount = sendQueuedResponsesOrPollForRequests();
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

    /**
     * Send a response, or if the publication cannot handle it queue up the sending of a response. This method
     * is thread safe.
     */
    void sendOkResponse(final long correlationId, final ControlResponseProxy proxy)
    {
        if (!proxy.sendResponse(correlationId, 0, OK, null, controlPublication))
        {
            queueResponse(correlationId, 0, OK, null);
        }
    }

    /**
     * Send a response, or if the publication cannot handle it queue up the sending of a response. This method
     * is thread safe.
     */
    void sendRecordingUnknown(final long correlationId, final long recordingId, final ControlResponseProxy proxy)
    {
        if (!proxy.sendResponse(correlationId, recordingId, RECORDING_UNKNOWN, null, controlPublication))
        {
            queueResponse(correlationId, recordingId, RECORDING_UNKNOWN, null);
        }
    }

    /**
     * Send a response, or if the publication cannot handle it queue up the sending of a response. This method
     * is thread safe.
     */
    void sendResponse(
        final long correlationId,
        final ControlResponseCode code,
        final String errorMessage,
        final ControlResponseProxy proxy)
    {
        if (!proxy.sendResponse(correlationId, 0, code, errorMessage, controlPublication))
        {
            queueResponse(correlationId, 0, code, errorMessage);
        }
    }

    /**
     * Send a descriptor, return the number of bytes sent or 0 if failed to send. This method is thread safe.
     */
    int sendDescriptor(
        final long correlationId,
        final UnsafeBuffer descriptorBuffer,
        final ControlResponseProxy proxy)
    {
        return proxy.sendDescriptor(correlationId, descriptorBuffer, controlPublication);
    }

    int maxPayloadLength()
    {
        return controlPublication.maxPayloadLength();
    }

    private int sendQueuedResponsesOrPollForRequests()
    {
        int workCount = 0;
        if (image.isClosed() || !controlPublication.isConnected())
        {
            state = State.INACTIVE;
        }
        else
        {
            if (responseQueue.isEmpty())
            {
                workCount += image.poll(adapter, FRAGMENT_LIMIT);
            }
            else
            {
                if (resend())
                {
                    responseQueue.poll();
                    timeoutDeadlineMs = -1;
                    workCount++;
                }
                else if (timeoutDeadlineMs == -1)
                {
                    timeoutDeadlineMs = epochClock.time() + TIMEOUT_MS;
                }
                else if (hasGoneInactive())
                {
                    state = State.INACTIVE;
                }
            }
        }

        return workCount;
    }

    private Boolean resend()
    {
        return responseQueue.peek().get();
    }

    private int waitForConnection()
    {
        int workCount = 0;

        if (controlPublication == null)
        {
            if (timeoutDeadlineMs == -1)
            {
                timeoutDeadlineMs = epochClock.time() + TIMEOUT_MS;
            }

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
            timeoutDeadlineMs = -1;
            state = State.ACTIVE;
            workCount += 1;
        }

        if (hasGoneInactive())
        {
            state = State.INACTIVE;
        }

        return workCount;
    }

    private boolean hasGoneInactive()
    {
        return timeoutDeadlineMs != -1 && epochClock.time() > timeoutDeadlineMs;
    }

    private void queueResponse(
        final long correlationId, final long relevantId, final ControlResponseCode code, final String message)
    {
        responseQueue.offer(
            () -> controlResponseProxy.sendResponse(correlationId, relevantId, code, message, controlPublication));
    }
}

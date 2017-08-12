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
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.SourceLocation;
import org.agrona.CloseHelper;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayDeque;
import java.util.function.Supplier;

import static io.aeron.archive.codecs.ControlResponseCode.*;

/**
 * Control sessions are interacted with from both the {@link ArchiveConductor} and the replay/record
 * {@link SessionWorker}s. The interaction may result in pending send actions being queued for execution by the
 * {@link ArchiveConductor}.
 * This complexity reflects the fact that replay/record/list requests happen in the context of a session, and that they
 * share the sessions request/reply channels. The relationship does not imply a lifecycle dependency however. A
 * {@link RecordingSession}/{@link ReplaySession} can outlive their 'parent' {@link ControlSession}.
 */
class ControlSession implements Session
{
    enum State
    {
        INIT, ACTIVE, INACTIVE, CLOSED
    }

    static final long TIMEOUT_MS = 5000L;
    private static final int NO_ACTIVE_DEADLINE = -1;

    private final ArchiveConductor conductor;
    private final EpochClock epochClock;
    private final ArrayDeque<AbstractListRecordingsSession> listRecordingsSessions = new ArrayDeque<>();
    private final ManyToOneConcurrentLinkedQueue<Supplier<Boolean>> queuedResponses =
        new ManyToOneConcurrentLinkedQueue<>();
    private final ControlResponseProxy controlResponseProxy;
    private final long controlSessionId;
    private final long correlationId;
    private final ControlSessionDemuxer demuxer;
    private final Publication controlPublication;
    private State state = State.INIT;
    private long timeoutDeadlineMs = -1;

    ControlSession(
        final long controlSessionId,
        final long correlationId,
        final ControlSessionDemuxer demuxer,
        final Publication controlPublication,
        final ArchiveConductor conductor,
        final EpochClock epochClock,
        final ControlResponseProxy controlResponseProxy)
    {
        this.controlSessionId = controlSessionId;
        this.correlationId = correlationId;
        this.demuxer = demuxer;
        this.controlPublication = controlPublication;
        this.conductor = conductor;
        this.epochClock = epochClock;
        this.controlResponseProxy = controlResponseProxy;
    }

    public long sessionId()
    {
        return controlSessionId;
    }

    public void abort()
    {
        state = State.INACTIVE;
    }

    public void close()
    {
        state = State.CLOSED;
        CloseHelper.quietClose(controlPublication);
        demuxer.notifyControlSessionClosed(this);
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

    public void onStopRecording(final long correlationId, final int streamId, final String channel)
    {
        conductor.stopRecording(correlationId, this, streamId, channel);
    }

    public void onStartRecording(
        final long correlationId, final String channel, final int streamId, final SourceLocation sourceLocation)
    {
        conductor.startRecordingSubscription(correlationId, this, streamId, channel, sourceLocation);
    }

    public void onListRecordingsForUri(
        final long correlationId,
        final long fromRecordingId,
        final int recordCount,
        final int streamId,
        final String channel)
    {
        final ListRecordingsForUriSession listRecordingsSession = conductor.newListRecordingsForUriSession(
            correlationId,
            fromRecordingId,
            recordCount,
            streamId,
            conductor.strippedChannelBuilder(channel).build(),
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
        final long recordingId,
        final long position,
        final long length,
        final int replayStreamId,
        final String replayChannel)
    {
        conductor.startReplay(
            correlationId,
            this,
            recordingId,
            position,
            length,
            replayStreamId,
            replayChannel);
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
        if (!proxy.sendResponse(controlSessionId, correlationId, 0, OK, null, controlPublication))
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
        if (!proxy.sendResponse(
            controlSessionId,
            correlationId,
            recordingId,
            RECORDING_UNKNOWN,
            null,
            controlPublication))
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
        if (!proxy.sendResponse(controlSessionId, correlationId, 0, code, errorMessage, controlPublication))
        {
            queueResponse(correlationId, 0, code, errorMessage);
        }
    }

    private void sendConnectResponse()
    {
        if (!controlResponseProxy.sendResponse(
            controlSessionId,
            correlationId,
            controlSessionId,
            CONNECTED,
            null,
            controlPublication))
        {
            queueResponse(correlationId, controlSessionId, CONNECTED, null);
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
        return proxy.sendDescriptor(controlSessionId, correlationId, descriptorBuffer, controlPublication);
    }

    int maxPayloadLength()
    {
        return controlPublication.maxPayloadLength();
    }

    private int sendQueuedResponsesOrPollForRequests()
    {
        int workCount = 0;
        if (!controlPublication.isConnected())
        {
            state = State.INACTIVE;
        }
        else
        {
            if (!queuedResponses.isEmpty())
            {
                if (sendFirst(queuedResponses))
                {
                    queuedResponses.poll();
                    timeoutDeadlineMs = NO_ACTIVE_DEADLINE;
                    workCount++;
                }
                else if (timeoutDeadlineMs == NO_ACTIVE_DEADLINE)
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

    private static boolean sendFirst(final ManyToOneConcurrentLinkedQueue<Supplier<Boolean>> responseQueue)
    {
        return responseQueue.peek().get();
    }

    private int waitForConnection()
    {
        int workCount = 0;
        if (timeoutDeadlineMs == NO_ACTIVE_DEADLINE)
        {
            timeoutDeadlineMs = epochClock.time() + TIMEOUT_MS;
        }
        else if (controlPublication.isConnected())
        {
            timeoutDeadlineMs = NO_ACTIVE_DEADLINE;
            state = State.ACTIVE;
            sendConnectResponse();
            workCount += 1;
        }
        else if (hasGoneInactive())
        {
            state = State.INACTIVE;
        }

        return workCount;
    }

    private boolean hasGoneInactive()
    {
        return timeoutDeadlineMs != NO_ACTIVE_DEADLINE && epochClock.time() > timeoutDeadlineMs;
    }

    private void queueResponse(
        final long correlationId, final long relevantId, final ControlResponseCode code, final String message)
    {
        queuedResponses.offer(
            () -> controlResponseProxy.sendResponse(
                controlSessionId,
                correlationId,
                relevantId,
                code,
                message,
                controlPublication));
    }
}

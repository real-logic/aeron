/*
 * Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.SourceLocation;
import org.agrona.CloseHelper;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayDeque;
import java.util.function.BooleanSupplier;

import static io.aeron.archive.client.ArchiveException.GENERIC;
import static io.aeron.archive.codecs.ControlResponseCode.*;

/**
 * Control sessions are interacted with from the {@link ArchiveConductor}. The interaction may result in pending
 * send actions being queued for execution by the {@link ArchiveConductor}.
 */
class ControlSession implements Session
{
    enum State
    {
        INIT, ACTIVE, INACTIVE, CLOSED
    }

    static final long TIMEOUT_MS = 5000L;

    private final ArchiveConductor conductor;
    private final EpochClock epochClock;
    private final ArrayDeque<BooleanSupplier> queuedResponses = new ArrayDeque<>(8);
    private final ControlResponseProxy controlResponseProxy;
    private final long controlSessionId;
    private final long correlationId;
    private final ControlSessionDemuxer demuxer;
    private final Publication controlPublication;
    private State state = State.INIT;
    private long activityDeadlineMs = -1;
    private AbstractListRecordingsSession activeListRecordingsSession;

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
        demuxer.removeControlSession(this);
        CloseHelper.close(controlPublication);
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
            workCount = sendQueuedResponses();
        }

        return workCount;
    }

    public AbstractListRecordingsSession activeListRecordingsSession()
    {
        return activeListRecordingsSession;
    }

    public void activeListRecordingsSession(final AbstractListRecordingsSession session)
    {
        activeListRecordingsSession = session;
    }

    public void onStopRecording(final long correlationId, final int streamId, final String channel)
    {
        conductor.stopRecording(correlationId, this, streamId, channel);
    }

    public void onStopRecordingSubscription(final long correlationId, final long subscriptionId)
    {
        conductor.stopRecordingSubscription(correlationId, this, subscriptionId);
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
        conductor.newListRecordingsForUriSession(
            correlationId,
            fromRecordingId,
            recordCount,
            streamId,
            channel,
            this);
    }

    public void onListRecordings(final long correlationId, final long fromRecordingId, final int recordCount)
    {
        conductor.newListRecordingsSession(correlationId, fromRecordingId, recordCount, this);
    }

    public void onListRecording(final long correlationId, final long recordingId)
    {
        conductor.listRecording(correlationId, this, recordingId);
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

    public void onStopReplay(final long correlationId, final long replaySessionId)
    {
        conductor.stopReplay(correlationId, this, replaySessionId);
    }

    public void onExtendRecording(
        final long correlationId,
        final long recordingId,
        final String channel,
        final int streamId,
        final SourceLocation sourceLocation)
    {
        conductor.extendRecording(correlationId, this, recordingId, streamId, channel, sourceLocation);
    }

    public void onGetRecordingPosition(final long correlationId, final long recordingId)
    {
        conductor.getRecordingPosition(correlationId, this, recordingId);
    }

    public void onTruncateRecording(final long correlationId, final long recordingId, final long position)
    {
        conductor.truncateRecording(correlationId, this, recordingId, position);
    }

    void onListRecordingSessionClosed(final AbstractListRecordingsSession listRecordingsSession)
    {
        if (listRecordingsSession != activeListRecordingsSession)
        {
            throw new ArchiveException();
        }

        activeListRecordingsSession = null;
    }

    void sendOkResponse(final long correlationId, final ControlResponseProxy proxy)
    {
        if (!proxy.sendResponse(controlSessionId, correlationId, 0L, OK, null, controlPublication))
        {
            queueResponse(correlationId, 0, OK, null);
        }
    }

    void sendOkResponse(final long correlationId, final long relevantId, final ControlResponseProxy proxy)
    {
        if (!proxy.sendResponse(controlSessionId, correlationId, relevantId, OK, null, controlPublication))
        {
            queueResponse(correlationId, relevantId, OK, null);
        }
    }

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

    void sendErrorResponse(final long correlationId, final String errorMessage, final ControlResponseProxy proxy)
    {
        if (!proxy.sendResponse(controlSessionId, correlationId, 0, ERROR, errorMessage, controlPublication))
        {
            queueResponse(correlationId, 0, ERROR, errorMessage);
        }
    }

    void sendErrorResponse(
        final long correlationId, final long relevantId, final String errorMessage, final ControlResponseProxy proxy)
    {
        if (!proxy.sendResponse(controlSessionId, correlationId, relevantId, ERROR, errorMessage, controlPublication))
        {
            queueResponse(correlationId, relevantId, ERROR, errorMessage);
        }
    }

    void attemptErrorResponse(final long correlationId, final String errorMessage, final ControlResponseProxy proxy)
    {
        proxy.attemptErrorResponse(controlSessionId, GENERIC, correlationId, errorMessage, controlPublication);
    }

    void attemptErrorResponse(
        final long correlationId, final long relevantId, final String errorMessage, final ControlResponseProxy proxy)
    {
        proxy.attemptErrorResponse(controlSessionId, correlationId, relevantId, errorMessage, controlPublication);
    }

    int sendDescriptor(final long correlationId, final UnsafeBuffer descriptorBuffer, final ControlResponseProxy proxy)
    {
        return proxy.sendDescriptor(controlSessionId, correlationId, descriptorBuffer, controlPublication);
    }

    int maxPayloadLength()
    {
        return controlPublication.maxPayloadLength();
    }

    private void sendConnectResponse()
    {
        if (!controlResponseProxy.sendResponse(
            controlSessionId,
            correlationId,
            controlSessionId,
            OK,
            null,
            controlPublication))
        {
            queueResponse(correlationId, controlSessionId, OK, null);
        }
    }

    private int sendQueuedResponses()
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
                    queuedResponses.pollFirst();
                    activityDeadlineMs = Aeron.NULL_VALUE;
                    workCount++;
                }
                else if (activityDeadlineMs == Aeron.NULL_VALUE)
                {
                    activityDeadlineMs = epochClock.time() + TIMEOUT_MS;
                }
                else if (hasGoneInactive())
                {
                    state = State.INACTIVE;
                }
            }
        }

        return workCount;
    }

    private static boolean sendFirst(final ArrayDeque<BooleanSupplier> responseQueue)
    {
        return responseQueue.peekFirst().getAsBoolean();
    }

    private int waitForConnection()
    {
        int workCount = 0;
        if (activityDeadlineMs == Aeron.NULL_VALUE)
        {
            activityDeadlineMs = epochClock.time() + TIMEOUT_MS;
        }
        else if (controlPublication.isConnected())
        {
            activityDeadlineMs = Aeron.NULL_VALUE;
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
        return activityDeadlineMs != Aeron.NULL_VALUE && epochClock.time() > activityDeadlineMs;
    }

    private void queueResponse(
        final long correlationId, final long relevantId, final ControlResponseCode code, final String message)
    {
        queuedResponses.offer(() -> controlResponseProxy.sendResponse(
            controlSessionId,
            correlationId,
            relevantId,
            code,
            message,
            controlPublication));
    }
}

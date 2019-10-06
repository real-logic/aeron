/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingTransitionType;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.exceptions.TimeoutException;
import org.agrona.CloseHelper;
import org.agrona.concurrent.EpochClock;

import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.RecordingTransitionType.REPLICATE;
import static io.aeron.archive.codecs.RecordingTransitionType.SYNC;

class ReplicationSession implements Session, RecordingDescriptorConsumer
{
    private static final String REPLICATION_ALIAS = "replication";

    enum State
    {
        CONNECT,
        AWAIT_DESCRIPTOR,
        AWAIT_RECORDING_POSITION,
        AWAIT_STOP_POSITION,
        AWAIT_REPLAY,
        EXTEND,
        AWAIT_REPLICATION,
        REPLICATE,
        DONE
    }

    private long activeCorrelationId = NULL_VALUE;
    private long srcReplaySessionId = NULL_VALUE;
    private long replayPosition = NULL_POSITION;
    private long stopPosition = NULL_POSITION;
    private long timeOfLastActionMs;
    private final long actionTimeoutMs;
    private final long correlationId;
    private final long replicationId;
    private final long srcRecordingId;
    private long dstRecordingId;
    private int replayStreamId;
    private final String replicationChannel;
    private final String liveDestination;
    private final EpochClock epochClock;
    private final ArchiveConductor conductor;
    private final ControlSession controlSession;
    private final ControlResponseProxy controlResponseProxy;
    private final Catalog catalog;
    private final Aeron aeron;
    private final AeronArchive.Context context;
    private AeronArchive.AsyncConnect asyncConnect;
    private AeronArchive srcArchive;
    private Subscription recordingSubscription;
    private Image image;
    private State state = State.CONNECT;

    ReplicationSession(
        final long correlationId,
        final long srcRecordingId,
        final long dstRecordingId,
        final long replicationId,
        final String liveDestination,
        final String replicationChannel,
        final RecordingSummary recordingSummary,
        final AeronArchive.Context context,
        final EpochClock epochClock,
        final Catalog catalog,
        final ControlResponseProxy controlResponseProxy,
        final ControlSession controlSession)
    {
        this.correlationId = correlationId;
        this.replicationId = replicationId;
        this.srcRecordingId = srcRecordingId;
        this.dstRecordingId = dstRecordingId;
        this.liveDestination = "".equals(liveDestination) ? null : liveDestination;
        this.replicationChannel = replicationChannel;
        this.aeron = context.aeron();
        this.context = context;
        this.catalog = catalog;
        this.controlResponseProxy = controlResponseProxy;
        this.epochClock = epochClock;
        this.conductor = controlSession.archiveConductor();
        this.controlSession = controlSession;
        this.actionTimeoutMs = TimeUnit.NANOSECONDS.toMillis(context.messageTimeoutNs());

        if (null != recordingSummary)
        {
            replayPosition = recordingSummary.stopPosition;
            replayStreamId = recordingSummary.streamId;
        }
    }

    public long sessionId()
    {
        return replicationId;
    }

    public boolean isDone()
    {
        return state == State.DONE;
    }

    public void abort()
    {
        this.state(State.DONE);
    }

    public void close()
    {
        controlSession.archiveConductor().removeReplicationSession(this);

        stopReplaySession();

        if (null != recordingSubscription)
        {
            conductor.removeRecordingSubscription(recordingSubscription.registrationId());
            recordingSubscription.close();
        }

        CloseHelper.close(asyncConnect);
        CloseHelper.close(srcArchive);
    }

    public int doWork()
    {
        int workCount = 0;

        try
        {
            switch (state)
            {
                case CONNECT:
                    workCount += connect();
                    break;

                case AWAIT_DESCRIPTOR:
                    workCount += awaitDescriptor();
                    break;

                case AWAIT_RECORDING_POSITION:
                    workCount += awaitRecordingPosition();
                    break;

                case AWAIT_STOP_POSITION:
                    workCount += awaitStopPosition();
                    break;

                case AWAIT_REPLAY:
                    workCount += awaitReplay();
                    break;

                case EXTEND:
                    workCount += extend();
                    break;

                case AWAIT_REPLICATION:
                    workCount += awaitReplication();
                    break;

                case REPLICATE:
                    workCount += replicate();
                    break;
            }
        }
        catch (final Throwable ex)
        {
            controlSession.sendErrorResponse(correlationId, ex.getMessage(), controlResponseProxy);
            state(State.DONE);
            error(ex);
            throw ex;
        }

        return workCount;
    }

    public void onRecordingDescriptor(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long startTimestamp,
        final long stopTimestamp,
        final long startPosition,
        final long stopPosition,
        final int initialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {
        replayPosition = startPosition;
        this.stopPosition = stopPosition;
        replayStreamId = streamId;

        dstRecordingId = catalog.addNewRecording(
            startPosition,
            startPosition,
            startTimestamp,
            startTimestamp,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity);

        notifyTransition(startPosition, REPLICATE);

        if (null != liveDestination && NULL_POSITION != stopPosition)
        {
            state(State.DONE);
            final ArchiveException ex = new ArchiveException("cannot live merge without active source recording");
            error(ex);
            throw ex;
        }

        State nextState = State.AWAIT_REPLAY;
        if (startPosition == stopPosition)
        {
            notifyTransition(stopPosition, SYNC);
            nextState = State.DONE;
        }

        state(nextState);
    }

    private int connect()
    {
        int workCount = 0;

        if (null == asyncConnect)
        {
            asyncConnect = AeronArchive.asyncConnect(context);
            workCount += 1;
        }
        else
        {
            final int step = asyncConnect.step();
            final AeronArchive archive = asyncConnect.poll();

            if (null == archive)
            {
                if (asyncConnect.step() != step)
                {
                    workCount += 1;
                }
            }
            else
            {
                srcArchive = archive;
                asyncConnect = null;
                state(NULL_VALUE == dstRecordingId ? State.AWAIT_DESCRIPTOR : State.AWAIT_RECORDING_POSITION);
                workCount += 1;
            }
        }

        return workCount;
    }

    private int awaitDescriptor()
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = aeron.nextCorrelationId();
            if (srcArchive.archiveProxy().listRecording(srcRecordingId, correlationId, srcArchive.controlSessionId()))
            {
                workCount += trackAction(correlationId);
                srcArchive.recordingDescriptorPoller().reset(correlationId, 1, this);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to list remote recording descriptor");
            }
        }
        else
        {
            final int fragments = srcArchive.recordingDescriptorPoller().poll();
            if (0 == fragments && epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to fetch remote recording descriptor");
            }

            workCount += fragments;
        }

        return workCount;
    }

    private int awaitRecordingPosition()
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = aeron.nextCorrelationId();
            if (srcArchive.archiveProxy().getRecordingPosition(
                srcRecordingId, correlationId, srcArchive.controlSessionId()))
            {
                workCount += trackAction(correlationId);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to send recording position request");
            }
        }
        else
        {
            final ControlResponsePoller poller = srcArchive.controlResponsePoller();
            workCount += poller.poll();

            if (hasResponse(poller))
            {
                State nextState = State.AWAIT_REPLAY;
                final long recordingPosition = poller.relevantId();
                if (NULL_POSITION == recordingPosition)
                {
                    if (null != liveDestination)
                    {
                        throw new ArchiveException("cannot live merge without active source recording");
                    }

                    nextState = State.AWAIT_STOP_POSITION;
                }

                state(nextState);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to get recording position");
            }
        }

        return workCount;
    }

    private int awaitStopPosition()
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = aeron.nextCorrelationId();
            if (srcArchive.archiveProxy().getStopPosition(srcRecordingId, correlationId, srcArchive.controlSessionId()))
            {
                workCount += trackAction(correlationId);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to send stop position request");
            }
        }
        else
        {
            final ControlResponsePoller poller = srcArchive.controlResponsePoller();
            workCount += poller.poll();

            if (hasResponse(poller))
            {
                State nexState = State.AWAIT_REPLAY;
                stopPosition = poller.relevantId();
                if (replayPosition == stopPosition)
                {
                    notifyTransition(stopPosition, SYNC);
                    nexState = State.DONE;
                }

                state(nexState);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to get stop position");
            }
        }

        return workCount;
    }

    private int awaitReplay()
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = aeron.nextCorrelationId();
            if (srcArchive.archiveProxy().replay(
                srcRecordingId,
                replayPosition,
                NULL_LENGTH,
                replicationChannel,
                replayStreamId,
                correlationId,
                srcArchive.controlSessionId()))
            {
                workCount += trackAction(correlationId);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to send replay request");
            }
        }
        else
        {
            final ControlResponsePoller poller = srcArchive.controlResponsePoller();
            workCount += poller.poll();

            if (hasResponse(poller))
            {
                srcReplaySessionId = poller.relevantId();
                state(State.EXTEND);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed get acknowledgement of replay request");
            }
        }

        return workCount;
    }

    private int extend()
    {
        final ChannelUri channelUri = ChannelUri.parse(replicationChannel);
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder();
        final String channel = builder
            .media(channelUri)
            .alias(REPLICATION_ALIAS)
            .controlMode(CommonContext.MDC_CONTROL_MODE_MANUAL)
            .rejoin(false)
            .sessionId((int)srcReplaySessionId)
            .build();

        recordingSubscription = conductor.extendRecording(
            replicationId, controlSession, dstRecordingId, replayStreamId, channel, SourceLocation.REMOTE);

        if (null == recordingSubscription)
        {
            state(State.DONE);
        }
        else
        {
            final String destination = builder.clear().media(channelUri).endpoint(channelUri).build();
            recordingSubscription.asyncAddDestination(destination);
            state(State.AWAIT_REPLICATION);
        }

        return 1;
    }

    private int awaitReplication()
    {
        int workCount = 0;

        image = recordingSubscription.imageBySessionId((int)srcReplaySessionId);
        if (null != image)
        {
            state(State.REPLICATE);
            workCount += 1;
        }
        else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
        {
            throw new TimeoutException("failed get replay image");
        }

        return workCount;
    }

    private int replicate()
    {
        int workCount = 0;

        if (recordingSubscription.isClosed())
        {
            state(State.DONE);
            return 1;
        }

        final long position = image.position();
        if (position == stopPosition || image.isClosed())
        {
            if (position == stopPosition)
            {
                srcReplaySessionId = NULL_VALUE;
                notifyTransition(position, SYNC);
            }

            state(State.DONE);
            workCount += 1;
        }

        return workCount;
    }

    private boolean hasResponse(final ControlResponsePoller poller)
    {
        if (poller.isPollComplete() && poller.controlSessionId() == srcArchive.controlSessionId())
        {
            final ControlResponseCode code = poller.code();
            if (ControlResponseCode.ERROR == code)
            {
                throw new ArchiveException(poller.errorMessage(), code.value());
            }

            return poller.correlationId() == activeCorrelationId && ControlResponseCode.OK == code;
        }

        return false;
    }

    private void error(final Throwable ex)
    {
        if (!controlSession.controlPublication().isConnected())
        {
            controlSession.sendErrorResponse(correlationId, ex.getMessage(), controlResponseProxy);
        }
    }

    private void notifyTransition(final long position, final RecordingTransitionType recordingTransitionType)
    {
        final long subscriptionId = null != recordingSubscription ? recordingSubscription.registrationId() : NULL_VALUE;
        controlSession.attemptSendTransition(
            replicationId, dstRecordingId, subscriptionId, position, recordingTransitionType);
    }

    private void stopReplaySession()
    {
        if (NULL_VALUE != srcReplaySessionId)
        {
            final long correlationId = aeron.nextCorrelationId();
            srcArchive.archiveProxy().stopReplay(srcReplaySessionId, correlationId, srcArchive.controlSessionId());
            srcReplaySessionId = NULL_VALUE;
        }
    }

    private int trackAction(final long correlationId)
    {
        timeOfLastActionMs = epochClock.time();
        activeCorrelationId = correlationId;
        return 1;
    }

    private void state(final State newState)
    {
        timeOfLastActionMs = epochClock.time();
        //System.out.println(timeOfLastActionMs + ": " + state + " -> " + newState);
        state = newState;
        activeCorrelationId = NULL_VALUE;
    }
}

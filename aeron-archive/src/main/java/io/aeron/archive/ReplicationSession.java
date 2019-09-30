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

class ReplicationSession implements Session, RecordingDescriptorConsumer
{
    private enum State
    {
        CONNECT,
        AWAIT_DESCRIPTOR,
        AWAIT_REPLAY,
        EXTEND,
        AWAIT_REPLICATION,
        REPLICATE,
        CLEANUP,
        DONE
    }

    private long activeCorrelationId = NULL_VALUE;
    private long srcReplaySessionId = NULL_VALUE;
    private long replayPosition;
    private long stopPosition = NULL_POSITION;
    private long timeOfLastActionMs;
    private final long actionTimeoutMs;
    private final long correlationId;
    private final long replicationId;
    private final long srcRecordingId;
    private long dstRecordingId;
    private final boolean liveMerge;
    private int replayStreamId;
    private final String replayChannel;
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
        final long replayPosition,
        final String replayChannel,
        final int replayStreamId,
        final boolean liveMerge,
        final long replicationId,
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
        this.replayPosition = replayPosition;
        this.replayChannel = replayChannel;
        this.replayStreamId = replayStreamId;
        this.liveMerge = liveMerge;
        this.aeron = context.aeron();
        this.context = context;
        this.catalog = catalog;
        this.controlResponseProxy = controlResponseProxy;
        this.epochClock = epochClock;
        this.conductor = controlSession.archiveConductor();
        this.controlSession = controlSession;
        this.actionTimeoutMs = TimeUnit.NANOSECONDS.toMillis(context.messageTimeoutNs());
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
        this.state(State.CLEANUP);
    }

    public void close()
    {
        controlSession.archiveConductor().removeReplicationSession(this);

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

                case CLEANUP:
                    workCount += cleanup();
                    break;
            }
        }
        catch (final Throwable ex)
        {
            state(State.CLEANUP);
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
        if (srcRecordingId != recordingId)
        {
            state(State.CLEANUP);
            throw new IllegalStateException("invalid recording id " + recordingId + " expected " + srcRecordingId);
        }

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

        replayPosition = startPosition;
        this.stopPosition = stopPosition;
        replayStreamId = streamId;
        activeCorrelationId = NULL_VALUE;

        controlSession.attemptSendRecordingTransition(
            replicationId, dstRecordingId, NULL_VALUE, startPosition, RecordingTransitionType.REPLICATE);

        state(State.AWAIT_REPLAY);
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
                state(NULL_VALUE == dstRecordingId ? State.AWAIT_DESCRIPTOR : State.AWAIT_REPLAY);
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
                timeOfLastActionMs = epochClock.time();
                activeCorrelationId = correlationId;
                srcArchive.recordingDescriptorPoller().reset(correlationId, 1, this);
                workCount += 1;
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
                replayChannel,
                replayStreamId,
                correlationId,
                srcArchive.controlSessionId()))
            {
                timeOfLastActionMs = epochClock.time();
                activeCorrelationId = correlationId;
                workCount += 1;
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
        final ChannelUri channelUri = ChannelUri.parse(replayChannel);
        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString((int)srcReplaySessionId));
        channelUri.put(CommonContext.REJOIN_PARAM_NAME, "false");
        final String channel = channelUri.toString();

        recordingSubscription = conductor.extendRecording(
            replicationId, controlSession, dstRecordingId, replayStreamId, channel, SourceLocation.REMOTE);

        if (null == recordingSubscription)
        {
            state(State.CLEANUP);
        }
        else
        {
            state(State.AWAIT_REPLICATION);
        }

        return 1;
    }

    private int awaitReplication()
    {
        image = recordingSubscription.imageBySessionId((int)srcReplaySessionId);
        if (null != image)
        {
            state(State.REPLICATE);
            return 1;
        }
        else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
        {
            throw new TimeoutException("failed get image for replay");
        }

        return 0;
    }

    private int replicate()
    {
        int workCount = 0;

        if (recordingSubscription.isClosed())
        {
            state(State.CLEANUP);
            return 1;
        }

        if (image.isClosed() || image.position() == stopPosition)
        {
            state(State.DONE);
            workCount += 1;
        }

        return workCount;
    }

    private int cleanup()
    {
        state(State.DONE);

        return 1;
    }

    private void error(final Throwable ex)
    {
        if (!controlSession.controlPublication().isConnected())
        {
            controlSession.sendErrorResponse(correlationId, ex.getMessage(), controlResponseProxy);
        }
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

    private void state(final State newState)
    {
        timeOfLastActionMs = epochClock.time();
        //System.out.println(timeOfLastActionMs + ": " + state + " -> " + newState);
        state = newState;
    }
}

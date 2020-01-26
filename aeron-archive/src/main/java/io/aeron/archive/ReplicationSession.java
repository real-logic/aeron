/*
 * Copyright 2014-2020 Real Logic Limited.
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
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.concurrent.CachedEpochClock;

import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.RecordingSignal.*;

class ReplicationSession implements Session, RecordingDescriptorConsumer
{
    private static final int LIVE_ADD_THRESHOLD = LogBufferDescriptor.TERM_MIN_LENGTH >> 2;
    private static final int REPLAY_REMOVE_THRESHOLD = 0;
    private static final int RETRY_ATTEMPTS = 3;

    enum State
    {
        CONNECT,
        REPLICATE_DESCRIPTOR,
        SRC_RECORDING_POSITION,
        REPLAY,
        EXTEND,
        AWAIT_IMAGE,
        REPLICATE,
        CATCHUP,
        ATTEMPT_LIVE_JOIN,
        DONE
    }

    private long activeCorrelationId = NULL_VALUE;
    private long srcReplaySessionId = NULL_VALUE;
    private long replayPosition = NULL_POSITION;
    private long srcStopPosition = NULL_POSITION;
    private long srcRecordingPosition = NULL_POSITION;
    private long timeOfLastActionMs;
    private final long actionTimeoutMs;
    private final long correlationId;
    private final long replicationId;
    private final long channelTagId;
    private final long subscriptionTagId;
    private final long srcRecordingId;
    private long dstRecordingId;
    private int replayStreamId;
    private int replaySessionId;
    private int retryAttempts = RETRY_ATTEMPTS;
    private boolean isLiveAdded;
    private final boolean isTagged;
    private final String replicationChannel;
    private final String liveDestination;
    private String replayDestination;
    private final CachedEpochClock epochClock;
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
        final long channelTagId,
        final long subscriptionTagId,
        final long replicationId,
        final String liveDestination,
        final String replicationChannel,
        final RecordingSummary recordingSummary,
        final AeronArchive.Context context,
        final CachedEpochClock epochClock,
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

        this.isTagged = NULL_VALUE != channelTagId || NULL_VALUE != subscriptionTagId;
        this.channelTagId = NULL_VALUE == channelTagId ? replicationId : channelTagId;
        this.subscriptionTagId = NULL_VALUE == subscriptionTagId ? replicationId : subscriptionTagId;

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
        stopRecording();
        stopReplaySession();

        CloseHelper.close(asyncConnect);
        CloseHelper.close(srcArchive);
        controlSession.archiveConductor().removeReplicationSession(this);
    }

    public int doWork()
    {
        int workCount = 0;

        try
        {
            if (null != recordingSubscription && recordingSubscription.isClosed())
            {
                state(State.DONE);
                return 1;
            }

            switch (state)
            {
                case CONNECT:
                    workCount += connect();
                    break;

                case REPLICATE_DESCRIPTOR:
                    workCount += replicateDescriptor();
                    break;

                case SRC_RECORDING_POSITION:
                    workCount += srcRecordingPosition();
                    break;

                case REPLAY:
                    workCount += replay();
                    break;

                case EXTEND:
                    workCount += extend();
                    break;

                case AWAIT_IMAGE:
                    workCount += awaitImage();
                    break;

                case REPLICATE:
                    workCount += replicate();
                    break;

                case CATCHUP:
                    workCount += catchup();
                    break;

                case ATTEMPT_LIVE_JOIN:
                    workCount += attemptLiveJoin();
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
        srcStopPosition = stopPosition;
        replayStreamId = streamId;
        replaySessionId = sessionId;

        if (NULL_VALUE == dstRecordingId)
        {
            replayPosition = startPosition;
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

            signal(startPosition, REPLICATE);
        }

        State nextState = State.REPLAY;

        if (null != liveDestination)
        {
            if (NULL_POSITION != stopPosition)
            {
                state(State.DONE);
                final ArchiveException ex = new ArchiveException("cannot live merge without active source recording");
                error(ex);
                throw ex;
            }

            nextState = State.SRC_RECORDING_POSITION;
        }

        if (startPosition == stopPosition)
        {
            signal(stopPosition, SYNC);
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
                state(State.REPLICATE_DESCRIPTOR);
                workCount += 1;
            }
        }

        return workCount;
    }

    private int replicateDescriptor()
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

    private int srcRecordingPosition()
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
                srcRecordingPosition = poller.relevantId();
                if (NULL_POSITION == srcRecordingPosition)
                {
                    if (null != liveDestination)
                    {
                        throw new ArchiveException("cannot live merge without active source recording");
                    }
                }

                state(State.REPLAY);
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                throw new TimeoutException("failed to get recording position");
            }
        }

        return workCount;
    }

    private int replay()
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = aeron.nextCorrelationId();
            final ChannelUri channelUri = ChannelUri.parse(replicationChannel);
            if (null != liveDestination)
            {
                channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(replaySessionId));
                channelUri.put(CommonContext.LINGER_PARAM_NAME, "0");
                channelUri.put(CommonContext.EOS_PARAM_NAME, "false");
            }

            if (srcArchive.archiveProxy().replay(
                srcRecordingId,
                replayPosition,
                Long.MAX_VALUE,
                channelUri.toString(),
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
                throw new TimeoutException("failed get acknowledgement of replay request to: " + replicationChannel);
            }
        }

        return workCount;
    }

    private int extend()
    {
        final ChannelUri channelUri = ChannelUri.parse(replicationChannel);
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media(channelUri)
            .alias(channelUri)
            .rejoin(false)
            .sessionId((int)srcReplaySessionId);

        if (isTagged || null != liveDestination)
        {
            builder.tags(channelTagId + "," + subscriptionTagId)
                .controlMode(CommonContext.MDC_CONTROL_MODE_MANUAL);
        }
        else
        {
            builder.endpoint(channelUri);
        }

        recordingSubscription = conductor.extendRecording(
            replicationId, dstRecordingId, replayStreamId, SourceLocation.REMOTE, builder.build(), controlSession);

        if (null == recordingSubscription)
        {
            state(State.DONE);
        }
        else
        {
            if (isTagged || null != liveDestination)
            {
                replayDestination = builder.clear().media(channelUri).endpoint(channelUri).build();
                recordingSubscription.asyncAddDestination(replayDestination);
            }

            state(State.AWAIT_IMAGE);
        }

        return 1;
    }

    private int awaitImage()
    {
        int workCount = 0;

        image = recordingSubscription.imageBySessionId((int)srcReplaySessionId);
        if (null != image)
        {
            state(null == liveDestination ? State.REPLICATE : State.CATCHUP);
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

        final long position = image.position();
        if ((NULL_VALUE != srcStopPosition && position >= srcStopPosition) || image.isClosed())
        {
            if ((NULL_VALUE != srcStopPosition && position >= srcStopPosition) ||
                (NULL_VALUE == srcStopPosition && image.isEndOfStream()))
            {
                srcReplaySessionId = NULL_VALUE;
                signal(position, SYNC);
            }

            state(State.DONE);
            workCount += 1;
        }

        return workCount;
    }

    private int catchup()
    {
        int workCount = 0;

        if (image.isClosed())
        {
            throw new ArchiveException("replication image closed unexpectedly");
        }

        if (image.position() >= srcRecordingPosition)
        {
            state(State.ATTEMPT_LIVE_JOIN);
            workCount += 1;
        }

        return workCount;
    }

    private int attemptLiveJoin()
    {
        int workCount = 0;

        if (image.isClosed())
        {
            throw new ArchiveException("replication image closed unexpectedly");
        }

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
                trackAction(NULL_VALUE);
                retryAttempts = RETRY_ATTEMPTS;
                srcRecordingPosition = poller.relevantId();

                if (NULL_POSITION == srcRecordingPosition)
                {
                    if (null != liveDestination)
                    {
                        throw new ArchiveException("cannot live merge without active source recording");
                    }
                }

                final long position = image.position();

                if (shouldAddLiveDestination(position))
                {
                    final String liveChannel = ChannelUri.addSessionId(liveDestination, replaySessionId);
                    recordingSubscription.asyncAddDestination(liveChannel);
                    isLiveAdded = true;
                }
                else if (shouldStopReplay(position))
                {
                    recordingSubscription.asyncRemoveDestination(replayDestination);
                    recordingSubscription = null;
                    signal(position, MERGE);
                    state(State.DONE);
                }

                workCount += 1;
            }
            else if (epochClock.time() >= (timeOfLastActionMs + actionTimeoutMs))
            {
                if (--retryAttempts == 0)
                {
                    throw new TimeoutException("failed to get recording position");
                }

                trackAction(NULL_VALUE);
            }
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

    private void signal(final long position, final RecordingSignal recordingSignal)
    {
        final long subscriptionId = null != recordingSubscription ? recordingSubscription.registrationId() : NULL_VALUE;
        controlSession.attemptSignal(replicationId, dstRecordingId, subscriptionId, position, recordingSignal);
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

    private void stopRecording()
    {
        if (null != recordingSubscription)
        {
            conductor.removeRecordingSubscription(recordingSubscription.registrationId());
            recordingSubscription.close();
            recordingSubscription = null;
        }
    }

    private boolean shouldAddLiveDestination(final long position)
    {
        return !isLiveAdded && (srcRecordingPosition - position) <= LIVE_ADD_THRESHOLD;
    }

    private boolean shouldStopReplay(final long position)
    {
        return isLiveAdded &&
            (srcRecordingPosition - position) <= REPLAY_REMOVE_THRESHOLD &&
            image.activeTransportCount() >= 2;
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
        //System.out.println("ReplicationSession: " + timeOfLastActionMs + ": " + state + " -> " + newState);
        state = newState;
        activeCorrelationId = NULL_VALUE;
    }
}

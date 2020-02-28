/*
 * Copyright 2014-2018 Real Logic Limited.
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
package io.aeron.archive.client;

import io.aeron.*;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.concurrent.EpochClock;

import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.*;

/**
 * Replay a recorded stream from a starting position and merge with live stream for a full history of a stream.
 * <p>
 * Once constructed either of {@link #poll(FragmentHandler, int)} or {@link #doWork()}, interleaved with consumption
 * of the {@link #image()}, should be called in a duty cycle loop until {@link #isMerged()} is {@code true}.
 * After which the {@link ReplayMerge} can be closed and continued usage can be made of the {@link Image} or its
 * parent {@link Subscription}. If an exception occurs or progress stops, the merge will fail and
 * {@link #hasFailed()} will be {@code true}.
 * <p>
 * NOTE: Merging is only supported with UDP streams.
 */
public class ReplayMerge implements AutoCloseable
{
    private static final long MERGE_PROGRESS_TIMEOUT_DEFAULT_MS = TimeUnit.SECONDS.toMillis(10);
    private static final int LIVE_ADD_THRESHOLD = LogBufferDescriptor.TERM_MIN_LENGTH >> 2;
    private static final int REPLAY_REMOVE_THRESHOLD = 0;

    enum State
    {
        GET_RECORDING_POSITION,
        REPLAY,
        CATCHUP,
        ATTEMPT_LIVE_JOIN,
        MERGED,
        FAILED,
        CLOSED
    }

    private final AeronArchive archive;
    private final Subscription subscription;
    private final EpochClock epochClock;
    private final String replayChannel;
    private final String replayDestination;
    private final String liveDestination;
    private final long recordingId;
    private final long startPosition;
    private final long mergeProgressTimeoutMs;

    private State state = State.GET_RECORDING_POSITION;
    private Image image;
    private long activeCorrelationId = Aeron.NULL_VALUE;
    private long nextTargetPosition = Aeron.NULL_VALUE;
    private long replaySessionId = Aeron.NULL_VALUE;
    private long positionOfLastProgress = Aeron.NULL_VALUE;
    private long timeOfLastProgressMs;
    private boolean isLiveAdded = false;
    private boolean isReplayActive = false;

    /**
     * Create a {@link ReplayMerge} to manage the merging of a replayed stream and switching over to live stream as
     * appropriate.
     *
     * @param subscription           to use for the replay and live stream. Must be a multi-destination subscription.
     * @param archive                to use for the replay.
     * @param replayChannel          to use for the replay.
     * @param replayDestination      to send the replay to and the destination added by the {@link Subscription}.
     * @param liveDestination        for the live stream and the destination added by the {@link Subscription}.
     * @param recordingId            for the replay.
     * @param startPosition          for the replay.
     * @param epochClock             to use for progress checks.
     * @param mergeProgressTimeoutMs to use for progress checks.
     */
    public ReplayMerge(
        final Subscription subscription,
        final AeronArchive archive,
        final String replayChannel,
        final String replayDestination,
        final String liveDestination,
        final long recordingId,
        final long startPosition,
        final EpochClock epochClock,
        final long mergeProgressTimeoutMs)
    {
        if (subscription.channel().startsWith(IPC_CHANNEL) ||
            replayChannel.startsWith(IPC_CHANNEL) ||
            replayDestination.startsWith(IPC_CHANNEL) ||
            liveDestination.startsWith(IPC_CHANNEL))
        {
            throw new IllegalArgumentException("IPC merging is not supported");
        }

        final ChannelUri subscriptionChannelUri = ChannelUri.parse(subscription.channel());
        if (!MDC_CONTROL_MODE_MANUAL.equals(subscriptionChannelUri.get(MDC_CONTROL_MODE_PARAM_NAME)))
        {
            throw new IllegalArgumentException("Subscription must have manual control-mode: control-mode=" +
                subscriptionChannelUri.get(MDC_CONTROL_MODE_PARAM_NAME));
        }

        final ChannelUri replayChannelUri = ChannelUri.parse(replayChannel);
        replayChannelUri.put(CommonContext.LINGER_PARAM_NAME, "0");
        replayChannelUri.put(CommonContext.EOS_PARAM_NAME, "false");

        this.archive = archive;
        this.subscription = subscription;
        this.epochClock = epochClock;
        this.replayDestination = replayDestination;
        this.replayChannel = replayChannelUri.toString();
        this.liveDestination = liveDestination;
        this.recordingId = recordingId;
        this.startPosition = startPosition;
        this.timeOfLastProgressMs = epochClock.time();
        this.mergeProgressTimeoutMs = mergeProgressTimeoutMs;

        subscription.asyncAddDestination(replayDestination);
    }

    /**
     * Create a {@link ReplayMerge} to manage the merging of a replayed stream and switching over to live stream as
     * appropriate.
     *
     * @param subscription      to use for the replay and live stream. Must be a multi-destination subscription.
     * @param archive           to use for the replay.
     * @param replayChannel     to use for the replay.
     * @param replayDestination to send the replay to and the destination added by the {@link Subscription}.
     * @param liveDestination   for the live stream and the destination added by the {@link Subscription}.
     * @param recordingId       for the replay.
     * @param startPosition     for the replay.
     */
    public ReplayMerge(
        final Subscription subscription,
        final AeronArchive archive,
        final String replayChannel,
        final String replayDestination,
        final String liveDestination,
        final long recordingId,
        final long startPosition)
    {
        this(
            subscription,
            archive,
            replayChannel,
            replayDestination,
            liveDestination,
            recordingId,
            startPosition,
            archive.context().aeron().context().epochClock(),
            MERGE_PROGRESS_TIMEOUT_DEFAULT_MS);
    }

    /**
     * Close and stop any active replay. Will remove the replay destination from the subscription.
     * This operation Will NOT remove the live destination if it has been added so it can be used for live consumption.
     */
    public void close()
    {
        final State state = this.state;
        if (State.CLOSED != state)
        {
            if (!archive.context().aeron().isClosed())
            {
                if (State.MERGED != state)
                {
                    subscription.asyncRemoveDestination(replayDestination);
                }

                if (isReplayActive && archive.archiveProxy().publication().isConnected())
                {
                    stopReplay();
                }
            }

            state(State.CLOSED);
        }
    }

    /**
     * Get the {@link Subscription} used to consume the replayed and merged stream.
     *
     * @return the {@link Subscription} used to consume the replayed and merged stream.
     */
    public Subscription subscription()
    {
        return subscription;
    }

    /**
     * Perform the work of replaying and merging. Should only be used if polling the underlying {@link Image} directly,
     * call {@link #poll(FragmentHandler, int)} on this class.
     *
     * @return indication of work done processing the merge.
     */
    public int doWork()
    {
        int workCount = 0;
        final long nowMs = epochClock.time();

        try
        {
            switch (state)
            {
                case GET_RECORDING_POSITION:
                    workCount += getRecordingPosition(nowMs);
                    checkProgress(nowMs);
                    break;

                case REPLAY:
                    workCount += replay(nowMs);
                    checkProgress(nowMs);
                    break;

                case CATCHUP:
                    workCount += catchup(nowMs);
                    checkProgress(nowMs);
                    break;

                case ATTEMPT_LIVE_JOIN:
                    workCount += attemptLiveJoin(nowMs);
                    checkProgress(nowMs);
                    break;
            }
        }
        catch (final Exception ex)
        {
            state(State.FAILED);
            throw ex;
        }

        return workCount;
    }

    /**
     * Poll the {@link Image} used for replay and merging and live stream. The {@link ReplayMerge#doWork()} method
     * will be called before the poll so that processing of the merge can be done.
     *
     * @param fragmentHandler to call for fragments.
     * @param fragmentLimit   for poll call.
     * @return number of fragments processed.
     */
    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        doWork();
        return null == image ? 0 : image.poll(fragmentHandler, fragmentLimit);
    }

    /**
     * Is the live stream merged and the replay stopped?
     *
     * @return true if live stream is merged and the replay stopped or false if not.
     */
    public boolean isMerged()
    {
        return state == State.MERGED;
    }

    /**
     * Has the replay merge failed due to an error?
     *
     * @return true if replay merge has failed due to an error.
     */
    public boolean hasFailed()
    {
        return state == State.FAILED;
    }

    /**
     * The {@link Image} which is a merge of the replay and live stream.
     *
     * @return the {@link Image} which is a merge of the replay and live stream.
     */
    public Image image()
    {
        return image;
    }

    /**
     * Is the live destination added to the {@link #subscription()}?
     *
     * @return true if live destination added or false if not.
     */
    public boolean isLiveAdded()
    {
        return isLiveAdded;
    }

    private int getRecordingPosition(final long nowMs)
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = archive.context().aeron().nextCorrelationId();

            if (archive.archiveProxy().getRecordingPosition(recordingId, correlationId, archive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                timeOfLastProgressMs = nowMs;
                workCount += 1;
            }
        }
        else if (pollForResponse(archive, activeCorrelationId))
        {
            nextTargetPosition = polledRelevantId(archive);
            activeCorrelationId = Aeron.NULL_VALUE;

            if (AeronArchive.NULL_POSITION == nextTargetPosition)
            {
                final long correlationId = archive.context().aeron().nextCorrelationId();

                if (archive.archiveProxy().getStopPosition(recordingId, correlationId, archive.controlSessionId()))
                {
                    activeCorrelationId = correlationId;
                    timeOfLastProgressMs = nowMs;
                    workCount += 1;
                }
            }
            else
            {
                timeOfLastProgressMs = nowMs;
                state(State.REPLAY);
            }

            workCount += 1;
        }

        return workCount;
    }

    private int replay(final long nowMs)
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = archive.context().aeron().nextCorrelationId();

            if (archive.archiveProxy().replay(
                recordingId,
                startPosition,
                Long.MAX_VALUE,
                replayChannel,
                subscription.streamId(),
                correlationId,
                archive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                timeOfLastProgressMs = nowMs;
                workCount += 1;
            }
        }
        else if (pollForResponse(archive, activeCorrelationId))
        {
            isReplayActive = true;
            replaySessionId = polledRelevantId(archive);
            timeOfLastProgressMs = nowMs;
            state(State.CATCHUP);
            workCount += 1;
        }

        return workCount;
    }

    private int catchup(final long nowMs)
    {
        int workCount = 0;

        if (null == image && subscription.isConnected())
        {
            timeOfLastProgressMs = nowMs;
            image = subscription.imageBySessionId((int)replaySessionId);
            positionOfLastProgress = null == image ? Aeron.NULL_VALUE : image.position();
        }

        if (null != image)
        {
            if (image.position() >= nextTargetPosition)
            {
                timeOfLastProgressMs = nowMs;
                state(State.ATTEMPT_LIVE_JOIN);
                workCount += 1;
            }
            else if (image.isClosed())
            {
                throw new IllegalStateException("ReplayMerge Image closed unexpectedly.");
            }
            else if (image.position() > positionOfLastProgress)
            {
                timeOfLastProgressMs = nowMs;
                positionOfLastProgress = image.position();
            }
        }

        return workCount;
    }

    private int attemptLiveJoin(final long nowMs)
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = archive.context().aeron().nextCorrelationId();

            if (archive.archiveProxy().getRecordingPosition(recordingId, correlationId, archive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                timeOfLastProgressMs = nowMs;
                workCount += 1;
            }
        }
        else if (pollForResponse(archive, activeCorrelationId))
        {
            nextTargetPosition = polledRelevantId(archive);
            activeCorrelationId = Aeron.NULL_VALUE;

            if (AeronArchive.NULL_POSITION == nextTargetPosition)
            {
                final long correlationId = archive.context().aeron().nextCorrelationId();

                if (archive.archiveProxy().getRecordingPosition(recordingId, correlationId, archive.controlSessionId()))
                {
                    activeCorrelationId = correlationId;
                    timeOfLastProgressMs = nowMs;
                }
            }
            else
            {
                State nextState = State.CATCHUP;

                if (null != image)
                {
                    final long position = image.position();

                    if (shouldAddLiveDestination(position))
                    {
                        subscription.asyncAddDestination(liveDestination);
                        timeOfLastProgressMs = nowMs;
                        isLiveAdded = true;
                    }
                    else if (shouldStopAndRemoveReplay(position))
                    {
                        subscription.asyncRemoveDestination(replayDestination);
                        stopReplay();
                        timeOfLastProgressMs = nowMs;
                        nextState = State.MERGED;
                    }
                }

                state(nextState);
            }

            workCount += 1;
        }

        return workCount;
    }

    private void stopReplay()
    {
        final long correlationId = archive.context().aeron().nextCorrelationId();
        if (archive.archiveProxy().stopReplay(replaySessionId, correlationId, archive.controlSessionId()))
        {
            isReplayActive = false;
        }
    }

    private void state(final ReplayMerge.State newState)
    {
        //System.out.println(state + " -> " + newState);
        state = newState;
        activeCorrelationId = Aeron.NULL_VALUE;
    }

    private boolean shouldAddLiveDestination(final long position)
    {
        return !isLiveAdded && (nextTargetPosition - position) <= LIVE_ADD_THRESHOLD;
    }

    private boolean shouldStopAndRemoveReplay(final long position)
    {
        return isLiveAdded &&
            (nextTargetPosition - position) <= REPLAY_REMOVE_THRESHOLD &&
            image.activeTransportCount() >= 2;
    }

    private boolean hasProgressStalled(final long nowMs)
    {
        return nowMs > (timeOfLastProgressMs + mergeProgressTimeoutMs);
    }

    private void checkProgress(final long nowMs)
    {
        if (hasProgressStalled(nowMs))
        {
            throw new TimeoutException("ReplayMerge no progress state=" + state);
        }
    }

    private static boolean pollForResponse(final AeronArchive archive, final long correlationId)
    {
        final ControlResponsePoller poller = archive.controlResponsePoller();
        if (poller.poll() > 0 && poller.isPollComplete())
        {
            if (poller.controlSessionId() == archive.controlSessionId())
            {
                if (poller.code() == ControlResponseCode.ERROR)
                {
                    throw new ArchiveException("archive response for correlationId=" + poller.correlationId() +
                        ", error: " + poller.errorMessage(),
                        (int)poller.relevantId(),
                        poller.correlationId());
                }

                return poller.correlationId() == correlationId;
            }
        }

        return false;
    }

    private static long polledRelevantId(final AeronArchive archive)
    {
        return archive.controlResponsePoller().relevantId();
    }

    public String toString()
    {
        return "ReplayMerge{" +
            "state=" + state +
            ", positionOfLastProgress=" + positionOfLastProgress +
            ", isLiveAdded=" + isLiveAdded +
            ", isReplayActive=" + isReplayActive +
            '}';
    }
}

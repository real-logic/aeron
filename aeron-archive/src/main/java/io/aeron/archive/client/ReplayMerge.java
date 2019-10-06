/*
 * Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_MANUAL;
import static io.aeron.CommonContext.MDC_CONTROL_MODE_PARAM_NAME;

/**
 * Replay a recorded stream from a starting position and merge with live stream for a full history of a stream.
 * <p>
 * Once constructed either of {@link #poll(FragmentHandler, int)} or {@link #doWork()}, interleaved with consumption
 * of the {@link #image()}, should be called in a duty cycle loop until {@link #isMerged()} is {@code true}.
 * After which the {@link ReplayMerge} can be closed and continued usage can be made of the {@link Image} or its
 * parent {@link Subscription}.
 */
public class ReplayMerge implements AutoCloseable
{
    private static final int LIVE_ADD_THRESHOLD = LogBufferDescriptor.TERM_MIN_LENGTH >> 2;
    private static final int REPLAY_REMOVE_THRESHOLD = 0;

    enum State
    {
        GET_RECORDING_POSITION,
        REPLAY,
        CATCHUP,
        ATTEMPT_LIVE_JOIN,
        STOP_REPLAY,
        MERGED,
        CLOSED
    }

    private final AeronArchive archive;
    private final Subscription subscription;
    private final String replayChannel;
    private final String replayDestination;
    private final String liveDestination;
    private final long recordingId;
    private final long startPosition;

    private State state = State.GET_RECORDING_POSITION;
    private Image image;
    private long activeCorrelationId = Aeron.NULL_VALUE;
    private long nextTargetPosition = Aeron.NULL_VALUE;
    private long replaySessionId = Aeron.NULL_VALUE;
    private boolean isLiveAdded = false;
    private boolean isReplayActive = false;

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
        final ChannelUri subscriptionChannelUri = ChannelUri.parse(subscription.channel());

        if (!MDC_CONTROL_MODE_MANUAL.equals(subscriptionChannelUri.get(MDC_CONTROL_MODE_PARAM_NAME)))
        {
            throw new IllegalArgumentException("Subscription channel must be manual control mode: mode=" +
                subscriptionChannelUri.get(MDC_CONTROL_MODE_PARAM_NAME));
        }

        this.archive = archive;
        this.subscription = subscription;
        this.replayDestination = replayDestination;
        this.replayChannel = replayChannel;
        this.liveDestination = liveDestination;
        this.recordingId = recordingId;
        this.startPosition = startPosition;

        subscription.asyncAddDestination(replayDestination);
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
                if (State.MERGED != state && State.STOP_REPLAY != state)
                {
                    subscription.removeDestination(replayDestination);
                }

                if (isReplayActive)
                {
                    isReplayActive = false;
                    final long correlationId = archive.context().aeron().nextCorrelationId();
                    archive.archiveProxy().stopReplay(replaySessionId, correlationId, archive.controlSessionId());
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

        switch (state)
        {
            case GET_RECORDING_POSITION:
                workCount += getRecordingPosition();
                break;

            case REPLAY:
                workCount += replay();
                break;

            case CATCHUP:
                workCount += catchup();
                break;

            case ATTEMPT_LIVE_JOIN:
                workCount += attemptLiveJoin();
                break;

            case STOP_REPLAY:
                workCount += stopReplay();
                break;
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

    private int getRecordingPosition()
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = archive.context().aeron().nextCorrelationId();

            if (archive.archiveProxy().getRecordingPosition(recordingId, correlationId, archive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
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
                    workCount += 1;
                }
            }
            else
            {
                state(State.REPLAY);
            }

            workCount += 1;
        }

        return workCount;
    }

    private int replay()
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
                workCount += 1;
            }
        }
        else if (pollForResponse(archive, activeCorrelationId))
        {
            isReplayActive = true;
            replaySessionId = polledRelevantId(archive);
            state(State.CATCHUP);
            workCount += 1;
        }

        return workCount;
    }

    private int catchup()
    {
        int workCount = 0;

        if (null == image && subscription.isConnected())
        {
            image = subscription.imageBySessionId((int)replaySessionId);
        }

        if (null != image && image.position() >= nextTargetPosition)
        {
            state(State.ATTEMPT_LIVE_JOIN);
            workCount += 1;
        }

        return workCount;
    }

    private int attemptLiveJoin()
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = archive.context().aeron().nextCorrelationId();

            if (archive.archiveProxy().getRecordingPosition(recordingId, correlationId, archive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
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
                        isLiveAdded = true;
                    }
                    else if (shouldStopAndRemoveReplay(position))
                    {
                        subscription.asyncRemoveDestination(replayDestination);
                        nextState = State.STOP_REPLAY;
                    }
                }

                state(nextState);
            }

            workCount += 1;
        }

        return workCount;
    }

    private int stopReplay()
    {
        int workCount = 0;
        final long correlationId = archive.context().aeron().nextCorrelationId();

        if (archive.archiveProxy().stopReplay(replaySessionId, correlationId, archive.controlSessionId()))
        {
            isReplayActive = false;
            state(State.MERGED);

            workCount += 1;
        }

        return workCount;
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

    private static boolean pollForResponse(final AeronArchive archive, final long correlationId)
    {
        final ControlResponsePoller poller = archive.controlResponsePoller();

        if (poller.poll() > 0 && poller.isPollComplete())
        {
            if (poller.controlSessionId() == archive.controlSessionId() && poller.correlationId() == correlationId)
            {
                if (poller.code() == ControlResponseCode.ERROR)
                {
                    throw new ArchiveException("archive response for correlationId=" + correlationId +
                        ", error: " + poller.errorMessage(), (int)poller.relevantId());
                }

                return true;
            }
        }

        return false;
    }

    private static long polledRelevantId(final AeronArchive archive)
    {
        return archive.controlResponsePoller().relevantId();
    }
}

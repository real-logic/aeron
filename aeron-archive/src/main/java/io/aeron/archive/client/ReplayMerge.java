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
 * Replay a recorded stream from a starting position and merge with live stream to consume a full history of a stream.
 * <p>
 * Once constructed the either of {@link #poll(FragmentHandler, int)} or {@link #doWork()} interleaved with consumption
 * of the {@link #image()} should be called in a duty cycle loop until {@link #isMerged()} is {@code true},
 * after which the {@link ReplayMerge} can be closed and continued usage can be made of the {@link Image} or its
 * parent {@link Subscription}.
 */
public class ReplayMerge implements AutoCloseable
{
    private static final int LIVE_ADD_THRESHOLD = LogBufferDescriptor.TERM_MIN_LENGTH / 4;
    private static final int REPLAY_REMOVE_THRESHOLD = 0;

    public enum State
    {
        AWAIT_INITIAL_RECORDING_POSITION,
        AWAIT_REPLAY,
        AWAIT_CATCH_UP,
        AWAIT_CURRENT_RECORDING_POSITION,
        AWAIT_STOP_REPLAY,
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
    private final long liveAddThreshold;
    private final long replayRemoveThreshold;

    private State state = State.AWAIT_INITIAL_RECORDING_POSITION;
    private Image image;
    private long activeCorrelationId = Aeron.NULL_VALUE;
    private long initialMaxPosition = Aeron.NULL_VALUE;
    private long nextTargetPosition = Aeron.NULL_VALUE;
    private long replaySessionId = Aeron.NULL_VALUE;
    private boolean isLiveAdded = false;
    private boolean isReplayActive = false;

    /**
     * Create a {@link ReplayMerge} to manage the merging of a replayed stream and switching to live stream as
     * appropriate.
     *
     * @param subscription to use for the replay and live stream. Must be a multi-destination subscription.
     * @param archive to use for the replay.
     * @param replayChannel to use for the replay.
     * @param replayDestination to send the replay to and the destination added by the {@link Subscription}.
     * @param liveDestination for the live stream and the destination added by the {@link Subscription}.
     * @param recordingId for the replay.
     * @param startPosition for the replay.
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
        this.liveAddThreshold = LIVE_ADD_THRESHOLD;
        this.replayRemoveThreshold = REPLAY_REMOVE_THRESHOLD;

        subscription.addDestination(replayDestination);
    }

    /**
     * Close the merge and stop any active replay. Will remove the replay destination from the subscription. Will
     * NOT remove the live destination if it has been added.
     */
    public void close()
    {
        final State state = this.state;
        if (State.CLOSED != state)
        {
            if (isReplayActive)
            {
                isReplayActive = false;
                archive.stopReplay(replaySessionId);
            }

            if (State.MERGED != state)
            {
                subscription.removeDestination(replayDestination);
            }

            state(State.CLOSED);
        }
    }

    /**
     * Process the operation of the merge. Do not call the processing of fragments on the subscription.
     *
     * @return indication of work done processing the merge.
     */
    public int doWork()
    {
        int workCount = 0;

        switch (state)
        {
            case AWAIT_INITIAL_RECORDING_POSITION:
                workCount += awaitInitialRecordingPosition();
                break;

            case AWAIT_REPLAY:
                workCount += awaitReplay();
                break;

            case AWAIT_CATCH_UP:
                workCount += awaitCatchUp();
                break;

            case AWAIT_CURRENT_RECORDING_POSITION:
                workCount += awaitUpdatedRecordingPosition();
                break;

            case AWAIT_STOP_REPLAY:
                workCount += awaitStopReplay();
                break;
        }

        return workCount;
    }

    /**
     * Poll the {@link Image} used for the merging replay and live stream. The {@link ReplayMerge#doWork()} method
     * will be called before the poll so that processing of the merge can be done.
     *
     * @param fragmentHandler to call for fragments
     * @param fragmentLimit for poll call
     * @return number of fragments processed.
     */
    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        doWork();
        return null == image ? 0 : image.poll(fragmentHandler, fragmentLimit);
    }

    /**
     * State of this {@link ReplayMerge}.
     *
     * @return state of this {@link ReplayMerge}.
     */
    public ReplayMerge.State state()
    {
        return state;
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
     * The {@link Image} used for the replay and live stream.
     *
     * @return the {@link Image} used for the replay and live stream.
     */
    public Image image()
    {
        return image;
    }

    /**
     * Is the live destination added to the subscription?
     *
     * @return true if live destination added or false if not.
     */
    public boolean isLiveAdded()
    {
        return isLiveAdded;
    }

    private int awaitInitialRecordingPosition()
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
                initialMaxPosition = nextTargetPosition;
                activeCorrelationId = Aeron.NULL_VALUE;
                state(State.AWAIT_REPLAY);
            }
            workCount += 1;
        }

        return workCount;
    }

    private int awaitReplay()
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
            activeCorrelationId = Aeron.NULL_VALUE;
            state(State.AWAIT_CATCH_UP);
            workCount += 1;
        }

        return workCount;
    }

    private int awaitCatchUp()
    {
        int workCount = 0;

        if (null == image && subscription.isConnected())
        {
            image = subscription.imageBySessionId((int)replaySessionId);
        }

        if (null != image && image.position() >= nextTargetPosition)
        {
            activeCorrelationId = Aeron.NULL_VALUE;
            state(State.AWAIT_CURRENT_RECORDING_POSITION);
            workCount += 1;
        }

        return workCount;
    }

    private int awaitUpdatedRecordingPosition()
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
                State nextState = State.AWAIT_CATCH_UP;

                if (null != image)
                {
                    final long position = image.position();

                    if (shouldAddLiveDestination(position))
                    {
                        subscription.addDestination(liveDestination);
                        isLiveAdded = true;
                    }
                    else if (shouldStopAndRemoveReplay(position))
                    {
                        nextState = State.AWAIT_STOP_REPLAY;
                    }
                }

                activeCorrelationId = Aeron.NULL_VALUE;
                state(nextState);
            }
            workCount += 1;
        }

        return workCount;
    }

    private int awaitStopReplay()
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = archive.context().aeron().nextCorrelationId();

            if (archive.archiveProxy().stopReplay(replaySessionId, correlationId, archive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                workCount += 1;
            }
        }
        else if (pollForResponse(archive, activeCorrelationId))
        {
            isReplayActive = false;
            replaySessionId = Aeron.NULL_VALUE;
            activeCorrelationId = Aeron.NULL_VALUE;
            subscription.removeDestination(replayDestination);
            state(State.MERGED);
            workCount += 1;
        }

        return workCount;
    }

    private void state(final ReplayMerge.State state)
    {
        //System.out.println(this.state + " -> " + state);
        this.state = state;
    }

    private boolean shouldAddLiveDestination(final long position)
    {
        return !isLiveAdded && (nextTargetPosition - position) <= liveAddThreshold;
    }

    private boolean shouldStopAndRemoveReplay(final long position)
    {
        return nextTargetPosition > initialMaxPosition &&
            isLiveAdded && (nextTargetPosition - position) <= replayRemoveThreshold;
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

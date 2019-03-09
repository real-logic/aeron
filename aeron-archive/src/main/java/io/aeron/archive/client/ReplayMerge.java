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
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.logbuffer.FragmentHandler;

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
    private final int sessionId;

    private State state = State.AWAIT_INITIAL_RECORDING_POSITION;
    private Image image;
    private long activeCorrelationId = Aeron.NULL_VALUE;
    private long initialMaxPosition = Aeron.NULL_VALUE;
    private long nextTargetPosition = Aeron.NULL_VALUE;
    private long replaySessionId = Aeron.NULL_VALUE;
    private boolean isLiveAdded = false;
    private boolean isReplayActive = false;

    public ReplayMerge(
        final Subscription subscription,
        final AeronArchive archive,
        final String replayChannel,
        final String replayDestination,
        final String liveDestination,
        final long recordingId,
        final long startPosition,
        final int sessionId,
        final int maxReceiverWindow)
    {
        this.archive = archive;
        this.subscription = subscription;
        this.replayDestination = replayDestination;
        this.replayChannel = replayChannel;
        this.liveDestination = liveDestination;
        this.recordingId = recordingId;
        this.startPosition = startPosition;
        this.liveAddThreshold = maxReceiverWindow / 2;
        this.replayRemoveThreshold = maxReceiverWindow / 4;
        this.sessionId = sessionId;

        subscription.addDestination(replayDestination);
    }

    public void close()
    {
        final State state = this.state;
        if (State.CLOSED != state)
        {
            state(State.CLOSED);

            if (isReplayActive)
            {
                archive.stopReplay(replaySessionId);
                isReplayActive = false;
            }

            if (State.MERGED != state)
            {
                subscription.removeDestination(replayDestination);
            }
        }
    }

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

    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        doWork();
        return null == image ? 0 : image.poll(fragmentHandler, fragmentLimit);
    }

    public ReplayMerge.State state()
    {
        return state;
    }

    public boolean isMerged()
    {
        return state == State.MERGED;
    }

    public Image image()
    {
        return image;
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
            initialMaxPosition = nextTargetPosition;
            activeCorrelationId = Aeron.NULL_VALUE;
            state(State.AWAIT_REPLAY);
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
            image = subscription.imageBySessionId(sessionId);
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

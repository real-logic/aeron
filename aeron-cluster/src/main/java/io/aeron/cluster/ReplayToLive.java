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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.cluster.client.ClusterException;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FragmentHandler;

class ReplayToLive implements AutoCloseable
{
    enum State
    {
        INIT,
        AWAIT_INITIAL_RECORDING_POSITION,
        AWAIT_REPLAY,
        AWAIT_CATCH_UP,
        AWAIT_UPDATED_RECORDING_POSITION,
        AWAIT_STOP_REPLAY,
        CAUGHT_UP,
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
    private final int streamId;
    private final int sessionId;

    private State state = State.INIT;
    private Image image;
    private long activeCorrelationId = Aeron.NULL_VALUE;
    private long nextTargetPosition = Aeron.NULL_VALUE;
    private boolean liveAdded = false;
    private boolean replayActive = false;

    ReplayToLive(
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
        this.streamId = subscription.streamId();
        this.sessionId = sessionId;
    }

    public void close()
    {
        if (replayActive)
        {
            archive.stopReplay(sessionId);
        }

        state(State.CLOSED);
    }

    int doWork()
    {
        int workCount = 0;

        switch (state)
        {
            case INIT:
                workCount += init();
                break;

            case AWAIT_INITIAL_RECORDING_POSITION:
                workCount += awaitInitialRecordingPosition();
                break;

            case AWAIT_REPLAY:
                workCount += awaitReplay();
                break;

            case AWAIT_CATCH_UP:
                workCount += awaitCatchUp();
                break;

            case AWAIT_UPDATED_RECORDING_POSITION:
                workCount += awaitUpdatedRecordingPosition();
                break;

            case AWAIT_STOP_REPLAY:
                workCount += awaitStopReplay();
                break;
        }

        return workCount;
    }

    int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        doWork();
        return null == image ? 0 : image.poll(fragmentHandler, fragmentLimit);
    }

    int boundedControlledPoll(
        final ControlledFragmentHandler fragmentHandler, final long maxPosition, final int fragmentLimit)
    {
        doWork();
        return null == image ? 0 : image.boundedControlledPoll(fragmentHandler, maxPosition, fragmentLimit);
    }

    boolean isCaughtUp()
    {
        return (state == State.CAUGHT_UP);
    }

    private int init()
    {
        subscription.addDestination(replayDestination);
        state(State.AWAIT_INITIAL_RECORDING_POSITION);
        return 1;
    }

    private int awaitInitialRecordingPosition()
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = archive.context().aeron().nextCorrelationId();

            if (archive.archiveProxy().getRecordingPosition(
                recordingId, correlationId, archive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                workCount += 1;
            }
        }
        else if (pollForResponse(archive, activeCorrelationId))
        {
            nextTargetPosition = pollerRelevantId(archive);
            state(State.AWAIT_REPLAY);
            activeCorrelationId = Aeron.NULL_VALUE;
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
                streamId,
                correlationId,
                archive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                workCount += 1;
            }

            replayActive = true;
        }
        else if (pollForResponse(archive, activeCorrelationId))
        {
            state(State.AWAIT_CATCH_UP);
            activeCorrelationId = Aeron.NULL_VALUE;
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
            state(State.AWAIT_UPDATED_RECORDING_POSITION);
            activeCorrelationId = Aeron.NULL_VALUE;
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

            if (archive.archiveProxy().getRecordingPosition(
                recordingId, correlationId, archive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                workCount += 1;
            }
        }
        else if (pollForResponse(archive, activeCorrelationId))
        {
            nextTargetPosition = pollerRelevantId(archive);
            State nextState = State.AWAIT_CATCH_UP;

            if (null != image)
            {
                final long position = image.position();

                if (shouldAddLiveDestination(position))
                {
                    subscription.addDestination(liveDestination);
                    liveAdded = true;
                }
                else if (shouldStopAndRemoveReplay(position))
                {
                    nextState = State.AWAIT_STOP_REPLAY;
                }
            }

            state(nextState);
            activeCorrelationId = Aeron.NULL_VALUE;
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

            if (archive.archiveProxy().stopReplay(
                sessionId, correlationId, archive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                workCount += 1;
            }
        }
        else if (pollForResponse(archive, activeCorrelationId))
        {
            replayActive = false;
            subscription.removeDestination(replayDestination);
            state(State.CAUGHT_UP);
            activeCorrelationId = Aeron.NULL_VALUE;
            workCount += 1;
        }

        return workCount;
    }

    private void state(final ReplayToLive.State state)
    {
        //System.out.println(this.state + " -> " + state);
        this.state = state;
    }

    private boolean shouldAddLiveDestination(final long position)
    {
        return (!liveAdded && (nextTargetPosition - position) <= liveAddThreshold);
    }

    private boolean shouldStopAndRemoveReplay(final long position)
    {
        return (liveAdded && (nextTargetPosition - position) <= replayRemoveThreshold);
    }

    private static boolean pollForResponse(final AeronArchive archive, final long correlationId)
    {
        final ControlResponsePoller poller = archive.controlResponsePoller();

        if (poller.poll() > 0 && poller.isPollComplete())
        {
            if (poller.controlSessionId() == archive.controlSessionId() &&
                poller.correlationId() == correlationId)
            {
                if (poller.code() == ControlResponseCode.ERROR)
                {
                    throw new ClusterException("archive response for correlationId=" + correlationId +
                        ", error: " + poller.errorMessage());
                }

                return true;
            }
        }

        return false;
    }

    private static long pollerRelevantId(final AeronArchive archive)
    {
        return archive.controlResponsePoller().relevantId();
    }
}

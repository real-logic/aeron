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
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.cluster.codecs.RecordingLogDecoder;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.status.RecordingPos.findCounterIdByRecording;

class LogCatchup implements AutoCloseable
{
    enum State
    {
        INIT,
        AWAIT_LEADER_CONNECTION,
        AWAIT_EXTEND_RECORDING,
        AWAIT_REPLAY,
        AWAIT_TRANSFER,
        AWAIT_STOP_EXTEND_RECORDING,
        DONE
    }

    private final MemberStatusPublisher memberStatusPublisher;
    private final ClusterMember[] clusterMembers;
    private final ConsensusModule.Context context;
    private final int leaderMemberId;
    private final int memberId;

    private AeronArchive.AsyncConnect leaderAsyncConnect;
    private AeronArchive leaderArchive;
    private AeronArchive localArchive;
    private String replayChannel;
    private String extendChannel;
    private State state = State.INIT;

    private final long leadershipTermId;
    private final long fromPosition;
    private final long localRecordingId;
    private long targetPosition = NULL_POSITION;
    private long leaderRecordingId = Aeron.NULL_VALUE;
    private long activeCorrelationId = Aeron.NULL_VALUE;
    private int recPosCounterId = CountersReader.NULL_COUNTER_ID;

    LogCatchup(
        final AeronArchive localArchive,
        final MemberStatusPublisher memberStatusPublisher,
        final ClusterMember[] clusterMembers,
        final int leaderMemberId,
        final int memberId,
        final long leadershipTermId,
        final long logRecordingId,
        final long logPosition,
        final ConsensusModule.Context context)
    {
        this.localArchive = localArchive;
        this.memberStatusPublisher = memberStatusPublisher;
        this.clusterMembers = clusterMembers;
        this.context = context;
        this.leaderMemberId = leaderMemberId;
        this.memberId = memberId;
        this.leadershipTermId = leadershipTermId;
        this.localRecordingId = logRecordingId;
        this.fromPosition = logPosition;
    }

    public void close()
    {
        CloseHelper.close(leaderArchive);
    }

    public int doWork()
    {
        int workCount = 0;

        switch (state)
        {
            case INIT:
                workCount += init();
                break;

            case AWAIT_LEADER_CONNECTION:
                workCount += awaitLeaderConnection();
                break;

            case AWAIT_EXTEND_RECORDING:
                workCount += awaitExtendRecording();
                break;

            case AWAIT_REPLAY:
                workCount += awaitReplay();
                break;

            case AWAIT_TRANSFER:
                workCount += awaitTransfer();
                break;

            case AWAIT_STOP_EXTEND_RECORDING:
                workCount += awaitStopExtendRecording();
                break;
        }

        return workCount;
    }

    public boolean isDone()
    {
        return State.DONE == state;
    }

    public long currentPosition()
    {
        if (recPosCounterId != CountersReader.NULL_COUNTER_ID)
        {
            return context.aeron().countersReader().getCounterValue(recPosCounterId);
        }

        return NULL_POSITION;
    }

    public long fromPosition()
    {
        return fromPosition;
    }

    public long targetPosition()
    {
        return targetPosition;
    }

    public long localRecordingId()
    {
        return localRecordingId;
    }

    public void onLeaderRecordingLog(final RecordingLogDecoder decoder)
    {
        if (State.AWAIT_LEADER_CONNECTION == state &&
            decoder.correlationId() == activeCorrelationId &&
            decoder.requestMemberId() == memberId &&
            decoder.leaderMemberId() == leaderMemberId)
        {
            final RecordingLogDecoder.EntriesDecoder entries = decoder.entries();

            if (!entries.hasNext())
            {
                throw new IllegalStateException("no recording log for leadershipTermId=" + leadershipTermId);
            }

            final RecordingLogDecoder.EntriesDecoder logEntry = entries.next();

            leaderRecordingId = logEntry.recordingId();
            targetPosition = logEntry.termBaseLogPosition();

            extendChannel = new ChannelUriStringBuilder()
                .media(CommonContext.UDP_MEDIA)
                .endpoint(clusterMembers[memberId].transferEndpoint())
                .build();

            replayChannel = extendChannel;
        }
    }

    private int init()
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = context.aeron().nextCorrelationId();

            if (memberStatusPublisher.recordingLogQuery(
                clusterMembers[leaderMemberId].publication(),
                correlationId,
                leaderMemberId,
                memberId,
                leadershipTermId,
                1,
                false))
            {
                activeCorrelationId = correlationId;
            }
        }

        if (null == leaderAsyncConnect)
        {
            final ChannelUriStringBuilder archiveControlRequestChannel = new ChannelUriStringBuilder()
                .media(CommonContext.UDP_MEDIA)
                .endpoint(clusterMembers[leaderMemberId].archiveEndpoint());

            final AeronArchive.Context leaderArchiveContext = context.archiveContext().clone()
                .controlRequestChannel(archiveControlRequestChannel.build())
                .controlResponseStreamId(localArchive.context().controlResponseStreamId() + 1);

            leaderAsyncConnect = AeronArchive.asyncConnect(leaderArchiveContext);
            workCount += 1;
        }

        if (Aeron.NULL_VALUE != activeCorrelationId)
        {
            state(State.AWAIT_LEADER_CONNECTION);
            workCount += 1;
        }

        return workCount;
    }

    private int awaitLeaderConnection()
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE != leaderRecordingId)
        {
            if (null == leaderArchive)
            {
                leaderArchive = leaderAsyncConnect.poll();
            }
            else
            {
                state(State.AWAIT_EXTEND_RECORDING);
                activeCorrelationId = Aeron.NULL_VALUE;
                workCount += 1;
            }
        }

        return workCount;
    }

    private int awaitExtendRecording()
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = context.aeron().nextCorrelationId();

            if (localArchive.archiveProxy().extendRecording(
                extendChannel,
                context.logStreamId(),
                SourceLocation.REMOTE,
                localRecordingId,
                correlationId,
                localArchive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                workCount += 1;
            }
        }
        else if (pollForResponse(localArchive, activeCorrelationId))
        {
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
            final long correlationId = context.aeron().nextCorrelationId();

            if (leaderArchive.archiveProxy().replay(
                leaderRecordingId,
                fromPosition,
                targetPosition - fromPosition,
                replayChannel,
                context.logStreamId(),
                correlationId,
                leaderArchive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                workCount = 1;
            }
        }
        else if (pollForResponse(leaderArchive, activeCorrelationId))
        {
            state(State.AWAIT_TRANSFER);
            activeCorrelationId = Aeron.NULL_VALUE;
            workCount += 1;
        }

        return workCount;
    }

    private int awaitTransfer()
    {
        int workCount = 0;

        if (CountersReader.NULL_COUNTER_ID == recPosCounterId)
        {
            recPosCounterId = findCounterIdByRecording(context.aeron().countersReader(), localRecordingId);
            if (CountersReader.NULL_COUNTER_ID != recPosCounterId)
            {
                workCount = 1;
            }
        }
        else if (currentPosition() >= targetPosition)
        {
            state(State.AWAIT_STOP_EXTEND_RECORDING);
            activeCorrelationId = Aeron.NULL_VALUE;
        }

        return workCount;
    }

    private int awaitStopExtendRecording()
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = context.aeron().nextCorrelationId();

            if (localArchive.archiveProxy().stopRecording(
                extendChannel,
                context.logStreamId(),
                correlationId,
                localArchive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                workCount = 1;
            }
        }
        else if (pollForResponse(localArchive, activeCorrelationId))
        {
            state(State.DONE);
            activeCorrelationId = Aeron.NULL_VALUE;
            workCount += 1;
        }

        return workCount;
    }

    private void state(final State state)
    {
        //System.out.println(this.state + " -> " + state);
        this.state = state;
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
                    throw new IllegalStateException("archive response for correlationId=" + correlationId +
                        ", error: " + poller.errorMessage());
                }

                return true;
            }
        }

        return false;
    }
}

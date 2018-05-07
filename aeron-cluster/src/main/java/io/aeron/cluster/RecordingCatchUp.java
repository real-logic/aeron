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

import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.service.RecordingLog;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

class RecordingCatchUp implements AutoCloseable
{
    private static final long NULL_RECORDING_ID = -1;
    private static final long NULL_CORRELATION_ID = -1;

    enum State
    {
        INIT,
        AWAITING_LEADER_RECOVERY_PLAN,
        AWAITING_LEADER_ARCHIVE_CONNECT,
        AWAITING_EXTEND_RECORDING,
        AWAITING_START_REPLAY,
        AWAITING_TRANSFER,
        DONE
    }

    private final MemberStatusPublisher memberStatusPublisher;
    private final ClusterMember[] clusterMembers;
    private final RecordingLog.RecoveryPlan localRecoveryPlan;
    private final CountersReader localCountersReader;
    private final ConsensusModule.Context context;
    private final int leaderMemberId;
    private final int memberId;

    private AeronArchive.AsyncConnect leaderAsyncConnect;
    private AeronArchive leaderArchive;
    private AeronArchive localArchive;
    private String replayChannel;
    private String extendChannel;

    private State state = State.INIT;

    private long logPosition = NULL_POSITION;
    private long queryRecoveryPlanCorrelationId = NULL_CORRELATION_ID;
    private long targetPosition = NULL_POSITION;
    private long fromPosition = NULL_POSITION;
    private long leaderRecordingId = NULL_RECORDING_ID;
    private long recordingIdToExtend = NULL_RECORDING_ID;
    private long extendRecordingCorrelationId = NULL_CORRELATION_ID;
    private long replayCorrelationId = NULL_CORRELATION_ID;
    private int recPosCounterId = CountersReader.NULL_COUNTER_ID;
    private boolean archiveResponded = false;

    RecordingCatchUp(
        final AeronArchive localArchive,
        final MemberStatusPublisher memberStatusPublisher,
        final ClusterMember[] clusterMembers,
        final int leaderMemberId,
        final int memberId,
        final RecordingLog.RecoveryPlan localRecoveryPlan,
        final ConsensusModule.Context context)
    {
        this.localArchive = localArchive;
        this.memberStatusPublisher = memberStatusPublisher;
        this.clusterMembers = clusterMembers;
        this.localRecoveryPlan = localRecoveryPlan;
        this.localCountersReader = context.aeron().countersReader();
        this.context = context;
        this.leaderMemberId = leaderMemberId;
        this.memberId = memberId;
    }

    public void close()
    {
        CloseHelper.close(leaderArchive);
    }

    public int doWork()
    {
        int workCount = 0;

        if (State.AWAITING_TRANSFER == state)
        {
            if (currentPosition() >= targetPosition)
            {
                state = State.DONE;
            }

            return workCount;
        }

        switch (state)
        {
            case INIT:
                workCount += queryRecoveryPlan();
                break;

            case AWAITING_LEADER_RECOVERY_PLAN:
                workCount += connectToLeaderArchive();
                break;

            case AWAITING_LEADER_ARCHIVE_CONNECT:
                workCount += tryExtendRecording();
                break;

            case AWAITING_EXTEND_RECORDING:
                workCount += tryStartReplay();
                break;

            case AWAITING_START_REPLAY:
                workCount += tryFindRecordPosCounter();
                break;

            case DONE:
                break;
        }

        return workCount;
    }

    public boolean isInInit()
    {
        return State.INIT == state;
    }

    public boolean isCaughtUp()
    {
        return State.DONE == state;
    }

    public long currentPosition()
    {
        if (recPosCounterId != CountersReader.NULL_COUNTER_ID)
        {
            return localCountersReader.getCounterValue(recPosCounterId);
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

    public long logPosition()
    {
        return logPosition;
    }

    public long recordingIdToExtend()
    {
        return recordingIdToExtend;
    }

    public void onLeaderRecoveryPlan(
        final long correlationId,
        final int requestMemberId,
        final int responseMemberId,
        final DirectBuffer data,
        final int offset,
        final int length)
    {
        if (State.AWAITING_LEADER_RECOVERY_PLAN == state &&
            correlationId == queryRecoveryPlanCorrelationId &&
            requestMemberId == memberId &&
            responseMemberId == leaderMemberId)
        {
            final RecordingLog.RecoveryPlan leaderRecoveryPlan = new RecordingLog.RecoveryPlan(data, offset);

            final RecordingLog.ReplayStep localLastStep =
                localRecoveryPlan.termSteps.get(localRecoveryPlan.termSteps.size() - 1);
            final RecordingLog.ReplayStep leaderLastStep =
                leaderRecoveryPlan.termSteps.get(leaderRecoveryPlan.termSteps.size() - 1);

            validateRecoveryPlans(leaderRecoveryPlan, leaderLastStep, localLastStep);

            leaderRecordingId = leaderLastStep.entry.recordingId;
            recordingIdToExtend = localLastStep.entry.recordingId;

            fromPosition = localLastStep.recordingStopPosition;
            targetPosition = leaderLastStep.recordingStopPosition;
            logPosition = leaderRecoveryPlan.lastTermBaseLogPosition + leaderRecoveryPlan.lastTermPositionAppended;

            // TODO: raise this channel as a configuration option
            final ChannelUri channelUri = ChannelUri.parse("aeron:udp?endpoint=localhost:3333");
            final String endpoint = channelUri.get(CommonContext.ENDPOINT_PARAM_NAME);

            final ChannelUriStringBuilder uriStringBuilder = new ChannelUriStringBuilder();

            // TODO: need the other params from the local recording

            uriStringBuilder
                .media(CommonContext.UDP_MEDIA)
                .endpoint(endpoint)
                .sessionId(localLastStep.recordingSessionId);

            extendChannel = uriStringBuilder.build();

            uriStringBuilder.clear()
                .media(CommonContext.UDP_MEDIA)
                .endpoint(endpoint)
                .sessionId(localLastStep.recordingSessionId);

            replayChannel = uriStringBuilder.build();
        }
    }

    private int queryRecoveryPlan()
    {
        final long correlationId = context.aeron().nextCorrelationId();

        if (memberStatusPublisher.recoveryPlanQuery(
            clusterMembers[leaderMemberId].publication(), correlationId, leaderMemberId, memberId))
        {
            queryRecoveryPlanCorrelationId = correlationId;
            state = State.AWAITING_LEADER_RECOVERY_PLAN;
        }

        return 1;
    }

    private int connectToLeaderArchive()
    {
        if (NULL_RECORDING_ID != recordingIdToExtend)
        {
            final ChannelUriStringBuilder archiveControlRequestChannel = new ChannelUriStringBuilder()
                .media(CommonContext.UDP_MEDIA)
                .endpoint(clusterMembers[leaderMemberId].archiveEndpoint());

            final AeronArchive.Context leaderArchiveContext = new AeronArchive.Context()
                .aeron(context.aeron())
                .controlRequestChannel(archiveControlRequestChannel.build())
                .controlResponseChannel(leaderArchive.context().controlResponseChannel())
                .controlResponseStreamId(leaderArchive.context().controlResponseStreamId() + 1);

            leaderAsyncConnect = AeronArchive.asyncConnect(leaderArchiveContext);

            state = State.AWAITING_LEADER_ARCHIVE_CONNECT;
        }

        return 1;
    }

    private int tryExtendRecording()
    {
        int workCount = 0;

        if (null == leaderArchive)
        {
            leaderArchive = leaderAsyncConnect.poll();

            return workCount;
        }

        if (NULL_CORRELATION_ID == extendRecordingCorrelationId)
        {
            final long correlationId = context.aeron().nextCorrelationId();

            if (localArchive.archiveProxy().extendRecording(
                extendChannel,
                context.logStreamId(),
                SourceLocation.REMOTE,
                recordingIdToExtend,
                correlationId,
                localArchive.controlSessionId()))
            {
                extendRecordingCorrelationId = correlationId;
                archiveResponded = false;
                state = State.AWAITING_EXTEND_RECORDING;
                workCount = 1;
            }
        }

        return workCount;
    }

    private int tryStartReplay()
    {
        int workCount = 0;

        if (!archiveResponded && !pollForArchiveResponse(localArchive, extendRecordingCorrelationId))
        {
            return workCount;
        }

        if (NULL_CORRELATION_ID == replayCorrelationId)
        {
            archiveResponded = true;

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
                replayCorrelationId = correlationId;
                archiveResponded = false;
                state = State.AWAITING_START_REPLAY;
                workCount = 1;
            }
        }

        return workCount;
    }

    private int tryFindRecordPosCounter()
    {
        int workCount = 0;

        if (!archiveResponded && !pollForArchiveResponse(leaderArchive, replayCorrelationId))
        {
            return workCount;
        }

        if (CountersReader.NULL_COUNTER_ID == recPosCounterId)
        {
            archiveResponded = true;

            recPosCounterId = RecordingPos.findCounterIdByRecording(localCountersReader, recordingIdToExtend);
            if (CountersReader.NULL_COUNTER_ID != recPosCounterId)
            {
                state = State.AWAITING_TRANSFER;
                workCount = 1;
            }
        }

        return workCount;
    }

    private void validateRecoveryPlans(
        final RecordingLog.RecoveryPlan leaderRecoveryPlan,
        final RecordingLog.ReplayStep leaderLastStep,
        final RecordingLog.ReplayStep localLastStep)
    {
        if (leaderRecoveryPlan.lastLeadershipTermId != localRecoveryPlan.lastLeadershipTermId)
        {
            throw new IllegalStateException(
                "lastLeadershipTermIds are not equal, can not catch up: leader=" +
                leaderRecoveryPlan.lastLeadershipTermId +
                " local=" +
                localRecoveryPlan.lastLeadershipTermId);
        }

        if (leaderRecoveryPlan.termSteps.size() != localRecoveryPlan.termSteps.size())
        {
            throw new IllegalStateException(
                "replay steps are not equal, can not catch up: leader=" +
                leaderRecoveryPlan.termSteps.size() +
                " local=" +
                localRecoveryPlan.termSteps.size());
        }

        if (localLastStep.entry.leadershipTermId != leaderLastStep.entry.leadershipTermId)
        {
            throw new IllegalStateException(
                "last step leadershipTermIds are not equal, can not catch up: leader=" +
                leaderLastStep.entry.leadershipTermId +
                " local=" +
                localLastStep.entry.leadershipTermId);
        }

        if (localLastStep.recordingStartPosition != leaderLastStep.recordingStartPosition)
        {
            throw new IllegalStateException(
                "last step local start position does not match leader last step start position");
        }
    }

    private static boolean pollForArchiveResponse(final AeronArchive archive, final long correlationId)
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

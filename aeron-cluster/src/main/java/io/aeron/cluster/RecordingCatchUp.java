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

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.RecordingLog;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersReader;

class RecordingCatchUp implements AutoCloseable
{
    private final AeronArchive dstArchive;
    private final AeronArchive srcArchive;
    private final CountersReader countersReader;
    private final long targetPosition;
    private final long fromPosition;
    private final long recordingIdToExtend;
    private final int counterId;

    RecordingCatchUp(
        final AeronArchive dstArchive,
        final CountersReader targetCounters,
        final long recordingIdToExtend,
        final AeronArchive srcArchive,
        final long recordingIdToReplay,
        final long fromPosition,
        final long toPosition,
        final String replayChannel,
        final int replayStreamId)
    {
        this.dstArchive = dstArchive;
        this.srcArchive = srcArchive;
        this.countersReader = targetCounters;
        this.targetPosition = toPosition;
        this.fromPosition = fromPosition;
        this.recordingIdToExtend = recordingIdToExtend;

        dstArchive.extendRecording(recordingIdToExtend, replayChannel, replayStreamId, SourceLocation.REMOTE);

        srcArchive.startReplay(
            recordingIdToReplay, fromPosition, toPosition - fromPosition, replayChannel, replayStreamId);

        int counterId = RecordingPos.findCounterIdByRecording(targetCounters, recordingIdToExtend);
        while (CountersReader.NULL_COUNTER_ID == counterId)
        {
            Thread.yield();
            counterId = RecordingPos.findCounterIdByRecording(targetCounters, recordingIdToExtend);
        }

        this.counterId = counterId;
    }

    public void close()
    {
        CloseHelper.close(srcArchive);
        CloseHelper.close(dstArchive);
    }

    public boolean isCaughtUp()
    {
        return currentPosition() >= targetPosition;
    }

    public long currentPosition()
    {
        return countersReader.getCounterValue(counterId);
    }

    public long fromPosition()
    {
        return fromPosition;
    }

    public long targetPosition()
    {
        return targetPosition;
    }

    public long recordingIdToExtend()
    {
        return recordingIdToExtend;
    }

    public static RecordingCatchUp catchUp(
        final AeronArchive.Context localArchiveContext,
        final RecordingLog.RecoveryPlan localRecoveryPlan,
        final ClusterMember leader,
        final CountersReader localCounters,
        final String replayChannel,
        final int replayStreamId)
    {
        final AeronCluster.Context leaderContext = new AeronCluster.Context()
            .clusterMemberEndpoints(leader.clientFacingEndpoint());
        final RecordingLog.RecoveryPlan leaderRecoveryPlan;

        try (AeronCluster aeronCluster = AeronCluster.connect(leaderContext))
        {
            leaderRecoveryPlan = new RecordingLog.RecoveryPlan(aeronCluster.getRecoveryPlan());
        }

        if (leaderRecoveryPlan.lastLeadershipTermId != localRecoveryPlan.lastLeadershipTermId)
        {
            throw new IllegalStateException(
                "lastLeadershipTermIds are not equal, can not catch up: leader=" +
                leaderRecoveryPlan.lastLeadershipTermId +
                " local=" +
                localRecoveryPlan.lastLeadershipTermId);
        }

        final RecordingLog.ReplayStep localLastStep =
            localRecoveryPlan.termSteps.get(localRecoveryPlan.termSteps.size() - 1);
        final RecordingLog.ReplayStep leaderLastStep =
            leaderRecoveryPlan.termSteps.get(leaderRecoveryPlan.termSteps.size() - 1);

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

        final long leaderRecordingId = leaderLastStep.entry.recordingId;
        final long localRecordingId = localLastStep.entry.recordingId;

        final long extendStartPosition = localLastStep.recordingStopPosition;  // TODO: probably needs to be queried
        final long extendStopPosition = leaderLastStep.recordingStopPosition;

        final ChannelUriStringBuilder archiveControlRequestChannel = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .endpoint(leader.archiveEndpoint());

        final AeronArchive.Context leaderArchiveContext = new AeronArchive.Context()
            .controlRequestChannel(archiveControlRequestChannel.build());

        final AeronArchive localArchive = AeronArchive.connect(localArchiveContext.clone());
        final AeronArchive leaderArchive = AeronArchive.connect(leaderArchiveContext);

        return new RecordingCatchUp(
            localArchive,
            localCounters,
            localRecordingId,
            leaderArchive,
            leaderRecordingId,
            extendStartPosition,
            extendStopPosition,
            replayChannel,
            replayStreamId);
    }
}

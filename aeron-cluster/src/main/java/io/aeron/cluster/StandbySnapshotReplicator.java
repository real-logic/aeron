/*
 * Copyright 2014-2023 Real Logic Limited.
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
package io.aeron.cluster;

import io.aeron.ChannelUri;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.RecordingSignal;
import org.agrona.CloseHelper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.status.RecordingPos.NULL_RECORDING_ID;

class StandbySnapshotReplicator implements AutoCloseable
{
    private final AeronArchive archive;
    private final RecordingLog recordingLog;
    private final int serviceCount;
    private final String archiveControlChannel;
    private final int archiveControlStreamId;
    private final String replicationChannel;
    private MultipleRecordingReplication recordingReplication;
    private List<RecordingLog.Entry> remoteSnapshotToReplicate;
    private boolean isComplete = false;

    StandbySnapshotReplicator(
        final AeronArchive archive,
        final RecordingLog recordingLog,
        final int serviceCount,
        final String archiveControlChannel,
        final int archiveControlStreamId,
        final String replicationChannel)
    {
        this.archive = archive;
        this.recordingLog = recordingLog;
        this.serviceCount = serviceCount;
        this.archiveControlChannel = archiveControlChannel;
        this.archiveControlStreamId = archiveControlStreamId;
        this.replicationChannel = replicationChannel;
    }

    static StandbySnapshotReplicator newInstance(
        final AeronArchive.Context archiveCtx,
        final RecordingLog recordingLog,
        final int serviceCount,
        final String archiveControlChannel,
        final int archiveControlStreamId,
        final String replicationChannel)
    {
        final AeronArchive archive = AeronArchive.connect(archiveCtx.clone());
        final StandbySnapshotReplicator standbySnapshotReplicator = new StandbySnapshotReplicator(
            archive, recordingLog, serviceCount, archiveControlChannel, archiveControlStreamId, replicationChannel);
        archive.context().recordingSignalConsumer(standbySnapshotReplicator::onSignal);
        return standbySnapshotReplicator;
    }

    int poll(final long nowNs)
    {
        int workCount = 0;

        if (null == recordingReplication)
        {
            workCount++;

            final Map<String, List<RecordingLog.Entry>> snapshotsByEndpoint = recordingLog.latestRemoteSnapshots(
                serviceCount);

            if (snapshotsByEndpoint.isEmpty())
            {
                isComplete = true;
                return workCount;
            }

            final List<List<RecordingLog.Entry>> snapshotsOrderedByLogPosition = new ArrayList<>(
                snapshotsByEndpoint.values());
            snapshotsOrderedByLogPosition.sort(Comparator.comparingLong(o -> o.get(0).logPosition));

            remoteSnapshotToReplicate = snapshotsOrderedByLogPosition.get(
                snapshotsOrderedByLogPosition.size() - 1);
            final String archiveEndpoint = remoteSnapshotToReplicate.get(0).archiveEndpoint;

            final String srcChannel = ChannelUri.createDestinationUri(archiveControlChannel, archiveEndpoint);

            recordingReplication = MultipleRecordingReplication.newInstance(
                archive,
                archiveControlStreamId,
                srcChannel,
                replicationChannel,
                TimeUnit.SECONDS.toNanos(10),
                TimeUnit.SECONDS.toNanos(1));

            for (int i = 0, n = remoteSnapshotToReplicate.size(); i < n; i++)
            {
                final RecordingLog.Entry entry = remoteSnapshotToReplicate.get(i);
                recordingReplication.addRecording(entry.recordingId, NULL_RECORDING_ID, NULL_POSITION);
            }

            workCount++;
        }

        workCount += recordingReplication.poll(nowNs);
        archive.pollForRecordingSignals();

        if (recordingReplication.isComplete())
        {
            for (int i = 0, n = remoteSnapshotToReplicate.size(); i < n; i++)
            {
                final RecordingLog.Entry entry = remoteSnapshotToReplicate.get(i);
                final long dstRecordingId = recordingReplication.completedDstRecordingId(entry.recordingId);
                recordingLog.appendSnapshot(
                    dstRecordingId,
                    entry.leadershipTermId,
                    entry.termBaseLogPosition,
                    entry.logPosition,
                    entry.timestamp,
                    entry.serviceId);
            }
            recordingLog.force(0);

            recordingReplication = null;
            isComplete = true;
        }

        return workCount;
    }

    boolean isComplete()
    {
        return isComplete;
    }

    void onSignal(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long subscriptionId,
        final long position,
        final RecordingSignal signal)
    {
        if (null != recordingReplication)
        {
            recordingReplication.onSignal(correlationId, recordingId, position, signal);
        }
    }

    public void close()
    {
        CloseHelper.quietClose(archive);
    }
}

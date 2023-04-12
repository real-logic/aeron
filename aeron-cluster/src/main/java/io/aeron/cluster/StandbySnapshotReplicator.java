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
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.cluster.client.ClusterException;
import io.aeron.exceptions.AeronException;
import org.agrona.CloseHelper;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    private final Object2ObjectHashMap<String, String> errorsByEndpoint = new Object2ObjectHashMap<>();
    private MultipleRecordingReplication recordingReplication;
    private ArrayList<SnapshotReplicationEntry> snapshotsToReplicate;
    private SnapshotReplicationEntry currentSnapshotToReplicate;
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
        final AeronArchive archive = AeronArchive.connect(archiveCtx.clone().errorHandler(null));
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

            if (null == snapshotsToReplicate)
            {
                snapshotsToReplicate = computeSnapshotsToReplicate();

                if (null == snapshotsToReplicate)
                {
                    isComplete = true;
                    return workCount;
                }
            }

            if (snapshotsToReplicate.isEmpty())
            {
                throw new ClusterException(
                    "failed to replicate any standby snapshots, errors: " + errorsByEndpoint,
                    AeronException.Category.WARN);
            }

            currentSnapshotToReplicate = snapshotsToReplicate.remove(0);
            final String srcChannel = ChannelUri.createDestinationUri(
                archiveControlChannel, currentSnapshotToReplicate.endpoint);

            final long progressTimeoutNs = archive.context().messageTimeoutNs() * 2;
            final long progressIntervalNs = progressTimeoutNs / 10;

            recordingReplication = MultipleRecordingReplication.newInstance(
                archive,
                archiveControlStreamId,
                srcChannel,
                replicationChannel,
                progressTimeoutNs,
                progressIntervalNs);

            for (int i = 0, n = currentSnapshotToReplicate.recordingLogEntries.size(); i < n; i++)
            {
                final RecordingLog.Entry entry = currentSnapshotToReplicate.recordingLogEntries.get(i);
                recordingReplication.addRecording(entry.recordingId, NULL_RECORDING_ID, NULL_POSITION);
            }

            workCount++;
        }

        try
        {
            workCount += recordingReplication.poll(nowNs);
            archive.pollForRecordingSignals();
        }
        catch (final ArchiveException | ClusterException ex)
        {
            errorsByEndpoint.put(currentSnapshotToReplicate.endpoint, ex.getMessage());
            CloseHelper.quietClose(recordingReplication);
            recordingReplication = null;
        }

        if (null != recordingReplication && recordingReplication.isComplete())
        {
            for (int i = 0, n = currentSnapshotToReplicate.recordingLogEntries.size(); i < n; i++)
            {
                final RecordingLog.Entry entry = currentSnapshotToReplicate.recordingLogEntries.get(i);
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

            CloseHelper.quietClose(recordingReplication);
            recordingReplication = null;
            isComplete = true;
        }

        return workCount;
    }

    private ArrayList<SnapshotReplicationEntry> computeSnapshotsToReplicate()
    {
        final Map<String, List<RecordingLog.Entry>> snapshotsByEndpoint = recordingLog.latestRemoteSnapshots(
            serviceCount);

        final ArrayList<SnapshotReplicationEntry> s;
        if (snapshotsByEndpoint.isEmpty())
        {
            s = null;
        }
        else
        {
            s = new ArrayList<>();

            snapshotsByEndpoint.forEach(
                (k, v) ->
                {
                    final long logPosition = v.get(0).logPosition;
                    s.add(new SnapshotReplicationEntry(k, logPosition, v));
                });

            s.sort(SnapshotReplicationEntry::compareTo);
        }

        return s;
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

    private static final class SnapshotReplicationEntry implements Comparable<SnapshotReplicationEntry>
    {
        private final String endpoint;
        private final long logPosition;
        private final List<RecordingLog.Entry> recordingLogEntries = new ArrayList<>();

        private SnapshotReplicationEntry(
            final String endpoint,
            final long logPosition,
            final List<RecordingLog.Entry> entries)
        {
            this.endpoint = endpoint;
            this.logPosition = logPosition;
            this.recordingLogEntries.addAll(entries);
        }

        public int compareTo(final SnapshotReplicationEntry o)
        {
            final int descendingOrderCompare = -Long.compare(logPosition, o.logPosition);
            if (0 != descendingOrderCompare)
            {
                return descendingOrderCompare;
            }

            return endpoint.compareTo(o.endpoint);
        }
    }
}

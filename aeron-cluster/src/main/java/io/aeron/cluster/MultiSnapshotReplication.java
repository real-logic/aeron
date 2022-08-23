/*
 * Copyright 2014-2022 Real Logic Limited.
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

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;

import java.util.ArrayList;
import java.util.List;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

class MultiSnapshotReplication implements AutoCloseable
{
    private final AeronArchive archive;
    private final int srcControlStreamId;
    private final String srcControlChannel;
    private final String replicationChannel;
    private final ArrayList<RecordingLog.Snapshot> snapshotsPending = new ArrayList<>();
    private final ArrayList<RecordingLog.Snapshot> snapshotsRetrieved = new ArrayList<>();
    private int snapshotCursor = 0;
    private SnapshotReplication snapshotReplication = null;

    MultiSnapshotReplication(
        final AeronArchive archive,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String replicationChannel)
    {
        this.archive = archive;
        this.srcControlStreamId = srcControlStreamId;
        this.srcControlChannel = srcControlChannel;
        this.replicationChannel = replicationChannel;
    }

    void addSnapshot(final RecordingLog.Snapshot snapshot)
    {
        snapshotsPending.add(snapshot);
    }

    int poll()
    {
        if (isComplete())
        {
            return 0;
        }

        int workDone = 0;
        if (null == snapshotReplication)
        {
            replicateCurrentSnapshot(true);
            workDone++;
        }
        else
        {
            if (snapshotReplication.isDone())
            {
                if (snapshotReplication.isComplete())
                {
                    final RecordingLog.Snapshot pending = snapshotsPending.get(snapshotCursor);
                    snapshotsRetrieved.add(retrievedSnapshot(pending, snapshotReplication.recordingId()));
                    snapshotCursor++;
                    snapshotReplication = null;
                }
                else
                {
                    replicateCurrentSnapshot(false);
                }
                workDone++;
            }
        }

        return workDone;
    }

    void onSignal(final long correlationId, final long recordingId, final long position, final RecordingSignal signal)
    {
        if (null != snapshotReplication)
        {
            snapshotReplication.onSignal(correlationId, recordingId, position, signal);
        }
    }

    boolean isComplete()
    {
        return snapshotCursor >= snapshotsPending.size();
    }

    List<RecordingLog.Snapshot> snapshotsRetrieved()
    {
        return snapshotsRetrieved;
    }

    static RecordingLog.Snapshot retrievedSnapshot(final RecordingLog.Snapshot pending, final long recordingId)
    {
        return new RecordingLog.Snapshot(
            recordingId,
            pending.leadershipTermId,
            pending.termBaseLogPosition,
            pending.logPosition,
            pending.timestamp,
            pending.serviceId);
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        if (null != snapshotReplication)
        {
            snapshotReplication.close(archive);
            snapshotReplication = null;
        }
    }

    private void replicateCurrentSnapshot(final boolean isNew)
    {
        final long replicationId = archive.replicate(
            snapshotsPending.get(snapshotCursor).recordingId,
            RecordingPos.NULL_RECORDING_ID,
            NULL_POSITION,
            srcControlStreamId,
            srcControlChannel,
            null,
            replicationChannel);

        snapshotReplication = new SnapshotReplication(replicationId, isNew);
    }
}

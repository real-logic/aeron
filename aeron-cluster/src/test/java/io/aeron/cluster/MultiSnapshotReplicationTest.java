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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.RecordingSignal.REPLICATE_END;
import static io.aeron.archive.codecs.RecordingSignal.SYNC;
import static io.aeron.archive.status.RecordingPos.NULL_RECORDING_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class MultiSnapshotReplicationTest
{
    private final AeronArchive archive = mock(AeronArchive.class);
    private final String srcChannel = "aeron:udp?endpoint=coming_from:8888";
    private final String replicationChannel = "aeron:udp?endpoint=going_to:8888";
    private final int srcStreamId = 892374;

    @Test
    void shouldReplicateTwoSnapshots()
    {
        final long replicationId0 = 89374;
        final long newRecordingId0 = 9823754293L;
        final long replicationId1 = 89375;
        final long newRecordingId1 = 7635445643L;

        final List<RecordingLog.Snapshot> snapshots = Arrays.asList(
            new RecordingLog.Snapshot(2, 3, 5, 7, 11, 0),
            new RecordingLog.Snapshot(17, 3, 5, 7, 31, -1));

        when(archive.replicate(eq(snapshots.get(0).recordingId), anyLong(), anyLong(), anyInt(), any(), any(), any()))
            .thenReturn(replicationId0);
        when(archive.replicate(eq(snapshots.get(1).recordingId), anyLong(), anyLong(), anyInt(), any(), any(), any()))
            .thenReturn(replicationId1);

        final MultiSnapshotReplication multiSnapshotReplication = new MultiSnapshotReplication(
            archive, srcStreamId, srcChannel, replicationChannel);

        multiSnapshotReplication.addSnapshot(snapshots.get(0));
        multiSnapshotReplication.addSnapshot(snapshots.get(1));

        assertEquals(1, multiSnapshotReplication.poll());
        verify(archive).replicate(
            snapshots.get(0).recordingId,
            NULL_RECORDING_ID,
            NULL_POSITION,
            srcStreamId,
            srcChannel,
            null,
            replicationChannel);

        multiSnapshotReplication.poll();
        verifyNoMoreInteractions(archive);

        multiSnapshotReplication.onSignal(replicationId0, newRecordingId0, 23423, SYNC);
        multiSnapshotReplication.poll();
        verifyNoMoreInteractions(archive);

        multiSnapshotReplication.onSignal(replicationId0, newRecordingId0, 23423, REPLICATE_END);
        multiSnapshotReplication.poll();
        verifyNoMoreInteractions(archive);

        multiSnapshotReplication.poll();
        verify(archive).replicate(
            snapshots.get(1).recordingId,
            NULL_RECORDING_ID,
            NULL_POSITION,
            srcStreamId,
            srcChannel,
            null,
            replicationChannel);

        multiSnapshotReplication.poll();
        verifyNoMoreInteractions(archive);

        multiSnapshotReplication.onSignal(replicationId1, newRecordingId1, 23423, SYNC);
        multiSnapshotReplication.poll();
        verifyNoMoreInteractions(archive);

        multiSnapshotReplication.onSignal(replicationId1, newRecordingId1, 23423, REPLICATE_END);
        multiSnapshotReplication.poll();
        verifyNoMoreInteractions(archive);

        assertTrue(multiSnapshotReplication.isComplete());
        assertEquals(0, multiSnapshotReplication.snapshotsRetrieved().get(0).serviceId);
        assertEquals(newRecordingId0, multiSnapshotReplication.snapshotsRetrieved().get(0).recordingId);
        assertEquals(-1, multiSnapshotReplication.snapshotsRetrieved().get(1).serviceId);
        assertEquals(newRecordingId1, multiSnapshotReplication.snapshotsRetrieved().get(1).recordingId);

        multiSnapshotReplication.poll();
        verifyNoMoreInteractions(archive);
    }

    @Test
    void shouldRetryFailedReplications()
    {
        final long replicationId0 = 89374;
        final long replicationId1 = 89375;
        final List<RecordingLog.Snapshot> snapshots = Arrays.asList(
            new RecordingLog.Snapshot(2, 3, 5, 7, 11, 13),
            new RecordingLog.Snapshot(17, 19, 23, 29, 31, 37));

        when(archive.replicate(eq(snapshots.get(0).recordingId), anyLong(), anyLong(), anyInt(), any(), any(), any()))
            .thenReturn(replicationId0);
        when(archive.replicate(eq(snapshots.get(1).recordingId), anyLong(), anyLong(), anyInt(), any(), any(), any()))
            .thenReturn(replicationId1);

        final MultiSnapshotReplication multiSnapshotReplication = new MultiSnapshotReplication(
            archive, srcStreamId, srcChannel, replicationChannel);

        multiSnapshotReplication.addSnapshot(snapshots.get(0));
        multiSnapshotReplication.addSnapshot(snapshots.get(1));

        multiSnapshotReplication.poll();
        verify(archive).replicate(
            eq(snapshots.get(0).recordingId), anyLong(), anyLong(), anyInt(), any(), any(), any());

        multiSnapshotReplication.poll();
        multiSnapshotReplication.onSignal(replicationId0, snapshots.get(0).recordingId, 23423, REPLICATE_END);
        multiSnapshotReplication.poll();
        verify(archive, times(2)).replicate(
            eq(snapshots.get(0).recordingId), anyLong(), anyLong(), anyInt(), any(), any(), any());

        multiSnapshotReplication.onSignal(replicationId0, snapshots.get(0).recordingId, 23423, SYNC);
        multiSnapshotReplication.onSignal(replicationId0, snapshots.get(0).recordingId, 23423, REPLICATE_END);
        multiSnapshotReplication.poll();

        multiSnapshotReplication.poll();
        verify(archive).replicate(
            eq(snapshots.get(1).recordingId), anyLong(), anyLong(), anyInt(), any(), any(), any());

        multiSnapshotReplication.onSignal(replicationId1, snapshots.get(1).recordingId, 23423, SYNC);
        multiSnapshotReplication.onSignal(replicationId1, snapshots.get(1).recordingId, 23423, REPLICATE_END);
        multiSnapshotReplication.poll();

        assertTrue(multiSnapshotReplication.isComplete());
        assertEquals(snapshots.get(0).recordingId, multiSnapshotReplication.snapshotsRetrieved().get(0).recordingId);
        assertEquals(snapshots.get(1).recordingId, multiSnapshotReplication.snapshotsRetrieved().get(1).recordingId);
    }

    @Test
    void closeWillCloseUnderlyingSnapshotReplication()
    {
        final List<RecordingLog.Snapshot> snapshots = Arrays.asList(
            new RecordingLog.Snapshot(2, 3, 5, 7, 11, 0),
            new RecordingLog.Snapshot(17, 3, 5, 7, 31, -1));

        when(archive.replicate(eq(snapshots.get(0).recordingId), anyLong(), anyLong(), anyInt(), any(), any(), any()))
            .thenReturn(1L);
        when(archive.replicate(eq(snapshots.get(1).recordingId), anyLong(), anyLong(), anyInt(), any(), any(), any()))
            .thenReturn(2L);

        final MultiSnapshotReplication multiSnapshotReplication = new MultiSnapshotReplication(
            archive, srcStreamId, srcChannel, replicationChannel);
        snapshots.forEach(multiSnapshotReplication::addSnapshot);

        multiSnapshotReplication.poll();

        verify(archive).replicate(
            anyLong(), anyLong(), anyLong(), anyInt(), any(), any(), any());

        multiSnapshotReplication.close();
        verify(archive).stopReplication(anyLong());

        multiSnapshotReplication.close();
        verifyNoMoreInteractions(archive);
    }
}
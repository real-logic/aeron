/*
 * Copyright 2014-2020 Real Logic Limited.
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
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_ID;
import static io.aeron.cluster.RecordingLog.ENTRY_TYPE_SNAPSHOT;
import static io.aeron.cluster.RecordingLog.ENTRY_TYPE_TERM;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class RecordingLogTest
{
    private static final File TEMP_DIR = new File(SystemUtil.tmpDirName());
    private boolean ignoreMissingRecordingFile = false;

    @After
    public void after()
    {
        IoUtil.delete(new File(TEMP_DIR, RecordingLog.RECORDING_LOG_FILE_NAME), ignoreMissingRecordingFile);
    }

    @Test
    public void shouldCreateNewIndex()
    {
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(0, recordingLog.entries().size());
        }
    }

    @Test
    public void shouldAppendAndThenReloadLatestSnapshot()
    {
        final RecordingLog.Entry entry = new RecordingLog.Entry(1, 3, 2, 777, 4, NULL_VALUE, ENTRY_TYPE_SNAPSHOT, 0);

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendSnapshot(
                entry.recordingId,
                entry.leadershipTermId,
                entry.termBaseLogPosition,
                777,
                entry.timestamp,
                SERVICE_ID);
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(1, recordingLog.entries().size());

            final RecordingLog.Entry snapshot = recordingLog.getLatestSnapshot(SERVICE_ID);
            assertEquals(entry.toString(), snapshot.toString());
        }
    }

    @Test
    public void shouldIgnoreIncompleteSnapshotInRecoveryPlan()
    {
        final int serviceCount = 1;

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendSnapshot(1L, 1L, 0, 777L, 0, 0);
            recordingLog.appendSnapshot(2L, 1L, 0, 777L, 0, SERVICE_ID);
            recordingLog.appendSnapshot(3L, 1L, 0, 777L, 0, 0);
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(3, recordingLog.entries().size());

            final AeronArchive mockArchive = mock(AeronArchive.class);
            final RecordingLog.RecoveryPlan recoveryPlan = recordingLog.createRecoveryPlan(mockArchive, serviceCount);
            assertEquals(2, recoveryPlan.snapshots.size());
            assertEquals(SERVICE_ID, recoveryPlan.snapshots.get(0).serviceId);
            assertEquals(2L, recoveryPlan.snapshots.get(0).recordingId);
            assertEquals(0, recoveryPlan.snapshots.get(1).serviceId);
            assertEquals(1L, recoveryPlan.snapshots.get(1).recordingId);
        }
    }

    @Test
    public void shouldAppendAndThenCommitTermPosition()
    {
        final long newPosition = 9999L;
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            final long recordingId = 1L;
            final long leadershipTermId = 1111L;
            final long logPosition = 2222L;
            final long timestamp = 3333L;

            recordingLog.appendTerm(recordingId, leadershipTermId, logPosition, timestamp);
            recordingLog.commitLogPosition(leadershipTermId, newPosition);
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(1, recordingLog.entries().size());

            final RecordingLog.Entry actualEntry = recordingLog.entries().get(0);
            assertEquals(newPosition, actualEntry.logPosition);
        }
    }

    @Test
    public void shouldTombstoneEntry()
    {
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            final RecordingLog.Entry entryOne = new RecordingLog.Entry(
                1L, 3, 2, NULL_POSITION, 4, 0, ENTRY_TYPE_TERM, 0);
            recordingLog.appendTerm(
                entryOne.recordingId, entryOne.leadershipTermId, entryOne.termBaseLogPosition, entryOne.timestamp);

            final RecordingLog.Entry entryTwo = new RecordingLog.Entry(
                1L, 4, 3, NULL_POSITION, 5, 0, ENTRY_TYPE_TERM, 0);
            recordingLog.appendTerm(
                entryTwo.recordingId, entryTwo.leadershipTermId, entryTwo.termBaseLogPosition, entryTwo.timestamp);

            recordingLog.tombstoneEntry(entryTwo.leadershipTermId, recordingLog.nextEntryIndex() - 1);
            assertEquals(1, recordingLog.entries().size());
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(1, recordingLog.entries().size());
            assertEquals(2, recordingLog.nextEntryIndex());
        }
    }

    @Test
    public void shouldCorrectlyOrderSnapshots()
    {
        ignoreMissingRecordingFile = true;

        final ArrayList<RecordingLog.Snapshot> snapshots = new ArrayList<>();
        final ArrayList<RecordingLog.Entry> entries = new ArrayList<>();

        addRecordingLogEntry(entries, ConsensusModule.Configuration.SERVICE_ID, 0, ENTRY_TYPE_TERM);
        addRecordingLogEntry(entries, 2, 4, ENTRY_TYPE_SNAPSHOT);
        addRecordingLogEntry(entries, 1, 5, ENTRY_TYPE_SNAPSHOT);
        addRecordingLogEntry(entries, 0, 6, ENTRY_TYPE_SNAPSHOT);
        addRecordingLogEntry(entries, ConsensusModule.Configuration.SERVICE_ID, 7, ENTRY_TYPE_SNAPSHOT);

        RecordingLog.addSnapshots(snapshots, entries, 3, entries.size() - 1);

        assertEquals(4, snapshots.size());
        assertEquals(ConsensusModule.Configuration.SERVICE_ID, snapshots.get(0).serviceId);
        assertEquals(0, snapshots.get(1).serviceId);
        assertEquals(1, snapshots.get(2).serviceId);
        assertEquals(2, snapshots.get(3).serviceId);
    }

    @Test
    public void shouldTombstoneLatestSnapshot()
    {
        final long termBaseLogPosition = 0L;
        final long logIncrement = 640L;
        long leadershipTermId = 7L;
        long logPosition = 0L;
        long timestamp = 1000L;
        long recordingId = 1L;

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendTerm(recordingId++, leadershipTermId, termBaseLogPosition, timestamp);

            timestamp += 1;
            logPosition += logIncrement;

            recordingLog.appendSnapshot(
                recordingId++, leadershipTermId, termBaseLogPosition, logPosition, timestamp, 0);
            recordingLog.appendSnapshot(
                recordingId++, leadershipTermId, termBaseLogPosition, logPosition, timestamp, SERVICE_ID);

            timestamp += 1;
            logPosition += logIncrement;

            recordingLog.appendSnapshot(
                recordingId++, leadershipTermId, termBaseLogPosition, logPosition, timestamp, 0);
            recordingLog.appendSnapshot(
                recordingId++, leadershipTermId, termBaseLogPosition, logPosition, timestamp, SERVICE_ID);

            leadershipTermId++;
            recordingLog.appendTerm(recordingId, leadershipTermId, logPosition, timestamp);

            assertTrue(recordingLog.tombstoneLatestSnapshot());
            assertEquals(4, recordingLog.entries().size());
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(4, recordingLog.entries().size());
            assertEquals(2L, recordingLog.getLatestSnapshot(0).recordingId);
            assertEquals(3L, recordingLog.getLatestSnapshot(SERVICE_ID).recordingId);

            assertTrue(recordingLog.tombstoneLatestSnapshot());
            assertEquals(2, recordingLog.entries().size());
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(2, recordingLog.entries().size());
            assertFalse(recordingLog.tombstoneLatestSnapshot());
            assertEquals(leadershipTermId, recordingLog.getTermEntry(leadershipTermId).leadershipTermId);
        }
    }

    private static void addRecordingLogEntry(
        final ArrayList<RecordingLog.Entry> entries,
        final int serviceId,
        final int recordingId,
        final int entryType)
    {
        entries.add(new RecordingLog.Entry(
            recordingId, 1, 1440, 2880, 0L, serviceId, entryType, entries.size()));
    }
}
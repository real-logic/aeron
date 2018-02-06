/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.cluster.service;

import org.agrona.IoUtil;
import org.junit.After;
import org.junit.Test;

import java.io.File;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.service.RecordingLog.ENTRY_TYPE_SNAPSHOT;
import static io.aeron.cluster.service.RecordingLog.ENTRY_TYPE_TERM;
import static io.aeron.cluster.service.RecordingLog.NULL_VALUE;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class RecordingLogTest
{
    private static final File TEMP_DIR = new File(IoUtil.tmpDirName());

    @After
    public void after()
    {
        IoUtil.delete(new File(TEMP_DIR, RecordingLog.RECORDING_INDEX_FILE_NAME), false);
    }

    @Test
    public void shouldCreateNewIndex()
    {
        final RecordingLog recordingLog = new RecordingLog(TEMP_DIR);

        assertThat(recordingLog.entries().size(), is(0));
    }

    @Test
    public void shouldAppendAndThenReloadLatestSnapshot()
    {
        final RecordingLog recordingLog = new RecordingLog(TEMP_DIR);
        final RecordingLog.Entry entry = new RecordingLog.Entry(1, 3, 2, 777, 4, NULL_VALUE, ENTRY_TYPE_SNAPSHOT, 0);

        recordingLog.appendSnapshot(entry.recordingId, entry.leadershipTermId, entry.logPosition, 777, entry.timestamp);

        final RecordingLog recordingLogTwo = new RecordingLog(TEMP_DIR);
        assertThat(recordingLogTwo.entries().size(), is(1));

        final RecordingLog.Entry snapshot = recordingLogTwo.getLatestSnapshot();
        assertEquals(entry.toString(), snapshot.toString());
    }

    @Test
    public void shouldAppendAndThenCommitTermPosition()
    {
        final RecordingLog recordingLog = new RecordingLog(TEMP_DIR);
        final RecordingLog.Entry entry = new RecordingLog.Entry(1, 3, 2, 777, 4, 0, ENTRY_TYPE_TERM, 0);

        recordingLog.appendTerm(
            entry.recordingId, entry.leadershipTermId, entry.logPosition, entry.timestamp, entry.memberIdVote);
        recordingLog.commitLeadershipTermPosition(entry.leadershipTermId, 777);

        final RecordingLog recordingLogTwo = new RecordingLog(TEMP_DIR);
        assertThat(recordingLogTwo.entries().size(), is(1));

        final RecordingLog.Entry actualEntry = recordingLogTwo.entries().get(0);
        assertEquals(entry.toString(), actualEntry.toString());
    }

    @Test
    public void shouldTombstoneEntry()
    {
        final RecordingLog recordingLog = new RecordingLog(TEMP_DIR);

        final RecordingLog.Entry entryOne = new RecordingLog.Entry(1, 3, 2, NULL_POSITION, 4, 0, ENTRY_TYPE_TERM, 0);
        recordingLog.appendTerm(
            entryOne.recordingId,
            entryOne.leadershipTermId,
            entryOne.logPosition,
            entryOne.timestamp,
            entryOne.memberIdVote);

        final RecordingLog.Entry entryTwo = new RecordingLog.Entry(2, 4, 3, NULL_POSITION, 5, 0, ENTRY_TYPE_TERM, 0);
        recordingLog.appendTerm(
            entryTwo.recordingId,
            entryTwo.leadershipTermId,
            entryTwo.logPosition,
            entryTwo.timestamp,
            entryTwo.memberIdVote);

        recordingLog.tombstoneEntry(entryTwo.leadershipTermId, recordingLog.nextEntryIndex() - 1);
        final RecordingLog recordingLogTwo = new RecordingLog(TEMP_DIR);
        assertThat(recordingLogTwo.entries().size(), is(1));
        assertThat(recordingLogTwo.nextEntryIndex(), is(2));
    }
}
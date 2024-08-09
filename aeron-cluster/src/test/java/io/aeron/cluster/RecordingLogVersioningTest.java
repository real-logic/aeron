/*
 * Copyright 2014-2024 Real Logic Limited.
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static io.aeron.cluster.RecordingLog.ENTRY_TYPE_SNAPSHOT;
import static io.aeron.cluster.RecordingLog.ENTRY_TYPE_STANDBY_SNAPSHOT;
import static io.aeron.cluster.RecordingLog.ENTRY_TYPE_TERM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class RecordingLogVersioningTest
{
    private static final byte[] CHARACTER_TABLE = new byte[27];
    public static final long FIXED_SEED_FOR_CONSISTENT_DATA = 892374458763L;

    @TempDir File tempDirA;
    @TempDir File tempDirB;

    static
    {
        for (int i = 0; i < 26; i++)
        {
            CHARACTER_TABLE[i] = (byte)(i + 97);
        }

        CHARACTER_TABLE[26] = (byte)'.';
    }

    private static class TestEntry
    {
        private final int entryType;
        private final long recordingId;
        private final long leadershipTermId;
        private final long termBaseLogPosition;
        private final long logPosition;
        private final long timestamp;
        private final int serviceId;
        private final String endpoint;

        TestEntry(
            final int entryType,
            final long recordingId,
            final long leadershipTermId,
            final long termBaseLogPosition,
            final long logPosition,
            final long timestamp,
            final int serviceId,
            final String endpoint)
        {

            this.entryType = entryType;
            this.recordingId = recordingId;
            this.leadershipTermId = leadershipTermId;
            this.termBaseLogPosition = termBaseLogPosition;
            this.logPosition = logPosition;
            this.timestamp = timestamp;
            this.serviceId = serviceId;
            this.endpoint = endpoint;
        }

        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            final TestEntry testEntry = (TestEntry)o;
            return entryType == testEntry.entryType && recordingId == testEntry.recordingId &&
                leadershipTermId == testEntry.leadershipTermId &&
                termBaseLogPosition == testEntry.termBaseLogPosition && logPosition == testEntry.logPosition &&
                timestamp == testEntry.timestamp && serviceId == testEntry.serviceId &&
                Objects.equals(endpoint, testEntry.endpoint);
        }

        public int hashCode()
        {
            return Objects.hash(
                entryType,
                recordingId,
                leadershipTermId,
                termBaseLogPosition,
                logPosition,
                timestamp,
                serviceId,
                endpoint);
        }
    }

    private List<TestEntry> generateData()
    {
        final Random r = new Random(FIXED_SEED_FOR_CONSISTENT_DATA);
        final byte[] bs = new byte[256];

        final long termRecordingId = r.nextInt(Integer.MAX_VALUE);
        final ArrayList<TestEntry> entries = new ArrayList<>();

        for (int i = 0; i < 2000; i++)
        {
            final int entryType = recordingType(r);
            final long recordingId = ENTRY_TYPE_TERM == entryType ? termRecordingId :
                (long)r.nextInt(Integer.MAX_VALUE);
            entries.add(new TestEntry(
                entryType,
                recordingId,
                r.nextLong(),
                r.nextLong(),
                r.nextLong(),
                r.nextLong(),
                r.nextInt(),
                endpoint(r, entryType, bs)));
        }

        return entries;
    }

    @Test
    void verifyDataRemainsConsistent()
    {
        assertEquals(generateData(), generateData());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldLoadOldVersionAndMigrate(final boolean hasPartiallyProcessedFiles) throws IOException
    {
        assertNotEquals(tempDirA, tempDirB);

        try (UnversionedRecordingLog recordingLog = new UnversionedRecordingLog(tempDirA, true))
        {
            for (final TestEntry testEntry : generateData())
            {
                recordingLog.append(
                    testEntry.entryType,
                    testEntry.recordingId,
                    testEntry.leadershipTermId,
                    testEntry.termBaseLogPosition,
                    testEntry.logPosition,
                    testEntry.timestamp,
                    testEntry.serviceId,
                    testEntry.endpoint);
            }
        }

        if (hasPartiallyProcessedFiles)
        {
            Files.createFile(new File(tempDirA, RecordingLog.RECORDING_LOG_MIGRATED_FILE_NAME).toPath());
            Files.createFile(new File(tempDirA, RecordingLog.RECORDING_LOG_NEW_FILE_NAME).toPath());
        }

        final RecordingLog recordingLogMigrated = new RecordingLog(tempDirA, false);
        final RecordingLog recordingLogNew = new RecordingLog(tempDirB, true);

        for (final TestEntry testEntry : generateData())
        {
            recordingLogNew.append(
                testEntry.entryType,
                testEntry.recordingId,
                testEntry.leadershipTermId,
                testEntry.termBaseLogPosition,
                testEntry.logPosition,
                testEntry.timestamp,
                testEntry.serviceId,
                testEntry.endpoint);
        }

        assertEquals(recordingLogMigrated.entries(), recordingLogNew.entries());
    }

    private int recordingType(final Random r)
    {
        final int type = r.nextInt(3);

        switch (type)
        {
            case 0: return ENTRY_TYPE_TERM;
            case 1: return ENTRY_TYPE_SNAPSHOT;
            case 2: return ENTRY_TYPE_STANDBY_SNAPSHOT;
        }

        throw new IllegalStateException();
    }

    private String endpoint(final Random r, final int type, final byte[] bs)
    {
        if (ENTRY_TYPE_STANDBY_SNAPSHOT == type)
        {
            final int length = 50 + r.nextInt(50);
            for (int i = 0; i < length; i++)
            {
                bs[i] = CHARACTER_TABLE[r.nextInt(CHARACTER_TABLE.length)];
            }
            return new String(bs, 0, length, StandardCharsets.US_ASCII) + ":" + r.nextInt(65536);
        }
        else
        {
            return null;
        }
    }
}

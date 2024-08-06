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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static io.aeron.cluster.RecordingLog.ENTRY_TYPE_TERM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordingLogVersioningTest
{
    private static final byte[] CHARACTER_TABLE = new byte[27];
    public static final long FIXED_SEED_FOR_CONSISTENT_DATA = 892374458763L;

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

        public TestEntry(
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

    @SuppressWarnings("checkstyle:MethodName")
    @Test
    @Disabled // Used to generate existing data.
    void generateRecordingLogForVersionTest_1_45()
    {
        final File parentDir = new File("src/test/resources/v1_45_x");
        try (UnversionedRecordingLog recordingLog = new UnversionedRecordingLog(parentDir, true))
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
            final List<UnversionedRecordingLog.Entry> entries = recordingLog.entries();
            final String s = entries.toString();
            System.out.println(s);
        }
    }

    private static void appendToRecordingLog(final RecordingLog recordingLog, final List<TestEntry> testEntries)
    {
        for (final TestEntry testEntry : testEntries)
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

    @Test
    void verifyDataRemainsConsistent()
    {
        assertEquals(generateData(), generateData());
    }

    @Test
    void shouldLoadOldVersionAndMigrate(@TempDir final File tempDirA, @TempDir final File tempDirB) throws IOException
    {
        assertNotEquals(tempDirA, tempDirB);

        final File parentDir = new File("src/test/resources/v1_45_x");
        final File oldFile = new File(parentDir, RecordingLog.RECORDING_LOG_FILE_NAME);
        final File tempOldFile = new File(tempDirA, RecordingLog.RECORDING_LOG_FILE_NAME);

        Files.copy(oldFile.toPath(), tempOldFile.toPath());

        final RecordingLog recordingLogMigrated = new RecordingLog(tempDirA, false);
        final RecordingLog recordingLogNew = new RecordingLog(tempDirB, true);
        appendToRecordingLog(recordingLogNew, generateData());

        assertEquals(recordingLogMigrated.entries(), recordingLogNew.entries());
    }

    private int recordingType(final Random r)
    {
        final int type = r.nextInt(3);

        switch (type)
        {
            case 0: return ENTRY_TYPE_TERM;
            case 1: return RecordingLog.ENTRY_TYPE_SNAPSHOT;
            case 2: return RecordingLog.ENTRY_TYPE_STANDBY_SNAPSHOT;
        }

        throw new IllegalStateException();
    }

    private String endpoint(final Random r, final int type, final byte[] bs)
    {
        if (RecordingLog.ENTRY_TYPE_STANDBY_SNAPSHOT == type)
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

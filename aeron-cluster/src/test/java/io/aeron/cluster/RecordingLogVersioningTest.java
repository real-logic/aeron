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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

import static io.aeron.cluster.RecordingLog.ENTRY_TYPE_TERM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordingLogVersioningTest
{
    @Test
    void generateRecordingLogForVersionTest()
    {
        final File parentDir = new File("src/test/resources/v1_45_x");
        final RecordingLog recordingLog = new RecordingLog(parentDir, true);
        final Random r = new Random(892374458763L);
        final byte[] bs = new byte[256];

        final long termRecordingId = r.nextInt(Integer.MAX_VALUE);

        for (int i = 0; i < 2000; i++)
        {
            final int entryType = recordingType(r);
            final long recordingId = ENTRY_TYPE_TERM == entryType ? termRecordingId : r.nextLong();
            recordingLog.append(
                entryType,
                recordingId,
                r.nextLong(),
                r.nextLong(),
                r.nextLong(),
                r.nextLong(),
                r.nextInt(),
                endpoint(r, entryType, bs));
        }

        final List<RecordingLog.Entry> entries = recordingLog.entries();
        final String s = textifyEntries(entries);

        final RecordingLog rNew = new RecordingLog(parentDir, false);
        final String sNew = textifyEntries(rNew.entries());

        assertEquals(s, sNew);

        System.out.println(s);
    }

    private static String textifyEntries(final List<RecordingLog.Entry> entries)
    {
        final StringBuilder builder = new StringBuilder();
        for (final RecordingLog.Entry entry : entries)
        {
            builder
                .append("entryIndex=").append(entry.entryIndex).append(',')
                .append("type=").append(entry.type).append(',')
                .append("recordingId=").append(entry.recordingId).append(',')
                .append("leadershipTermId=").append(entry.leadershipTermId).append(',')
                .append("termBaseLogPosition=").append(entry.termBaseLogPosition).append(',')
                .append("logPosition=").append(entry.logPosition).append(',')
                .append("timestamp=").append(entry.timestamp).append(',')
                .append("serviceId=").append(entry.serviceId).append(',')
                .append("archiveEndpoint=").append(entry.archiveEndpoint).append('\n');
        }

        return builder.toString();
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
                bs[i] = (byte)(r.nextInt(90-48) + 48);
            }
            return new String(bs, 0, length, StandardCharsets.US_ASCII);
        }
        else
        {
            return null;
        }
    }
}

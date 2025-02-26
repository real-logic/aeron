/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.driver.reports;

import org.agrona.BitUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.nio.ByteBuffer;

import static io.aeron.driver.reports.LossReport.*;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class LossReportTest
{
    private static final int CAPACITY = 1024;
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(CAPACITY));
    private final AtomicBuffer buffer = spy(unsafeBuffer);
    private final LossReport lossReport = new LossReport(buffer);

    @Test
    void shouldCreateEntry()
    {
        final long initialBytesLost = 32;
        final int timestampMs = 7;
        final int sessionId = 3;
        final int streamId = 1;
        final String channel = "aeron:udp://stuff";
        final String source = "127.0.0.1:8888";

        assertNotNull(lossReport.createEntry(initialBytesLost, timestampMs, sessionId, streamId, channel, source));

        final InOrder inOrder = inOrder(buffer);
        inOrder.verify(buffer).putLong(TOTAL_BYTES_LOST_OFFSET, initialBytesLost);
        inOrder.verify(buffer).putLong(FIRST_OBSERVATION_OFFSET, timestampMs);
        inOrder.verify(buffer).putLong(LAST_OBSERVATION_OFFSET, timestampMs);
        inOrder.verify(buffer).putInt(SESSION_ID_OFFSET, sessionId);
        inOrder.verify(buffer).putInt(STREAM_ID_OFFSET, streamId);
        inOrder.verify(buffer).putStringAscii(CHANNEL_OFFSET, channel);
        inOrder.verify(buffer).putStringAscii(
            CHANNEL_OFFSET + BitUtil.align(SIZE_OF_INT + channel.length(), SIZE_OF_INT), source);
        inOrder.verify(buffer).putLongRelease(OBSERVATION_COUNT_OFFSET, 1L);
    }

    @Test
    void shouldUpdateEntry()
    {
        final long initialBytesLost = 32;
        final int timestampMs = 7;
        final int sessionId = 3;
        final int streamId = 1;
        final String channel = "aeron:udp://stuff";
        final String source = "127.0.0.1:8888";

        final ReportEntry entry = lossReport.createEntry(
            initialBytesLost, timestampMs, sessionId, streamId, channel, source);

        final long additionBytesLost = 64;
        final long latestTimestamp = 10;
        entry.recordObservation(additionBytesLost, latestTimestamp);

        assertEquals(latestTimestamp, unsafeBuffer.getLong(LAST_OBSERVATION_OFFSET));
        assertEquals(initialBytesLost + additionBytesLost, unsafeBuffer.getLong(TOTAL_BYTES_LOST_OFFSET));
        assertEquals(2L, unsafeBuffer.getLong(OBSERVATION_COUNT_OFFSET));
    }
}

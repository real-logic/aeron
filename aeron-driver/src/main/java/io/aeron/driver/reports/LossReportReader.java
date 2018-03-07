/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.driver.reports;

import org.agrona.BitUtil;
import org.agrona.concurrent.AtomicBuffer;

import static io.aeron.driver.reports.LossReport.*;
import static org.agrona.BitUtil.SIZE_OF_INT;

/**
 * Reader that provides the function to read entries from a {@link LossReport}.
 */
public class LossReportReader
{
    /**
     * Read a {@link LossReport} contained in the buffer. This can be done concurrently.
     *
     * @param buffer        containing the loss report.
     * @param entryConsumer to be called to accept each entry in the report.
     * @return the number of entries read.
     */
    public static int read(final AtomicBuffer buffer, final EntryConsumer entryConsumer)
    {
        final int capacity = buffer.capacity();

        int recordsRead = 0;
        int offset = 0;

        while (offset < capacity)
        {
            final long observationCount = buffer.getLongVolatile(offset + OBSERVATION_COUNT_OFFSET);
            if (observationCount <= 0)
            {
                break;
            }

            ++recordsRead;

            final String channel = buffer.getStringAscii(offset + CHANNEL_OFFSET);
            final String source = buffer.getStringAscii(offset + CHANNEL_OFFSET + SIZE_OF_INT + channel.length());

            entryConsumer.accept(
                observationCount,
                buffer.getLong(offset + TOTAL_BYTES_LOST_OFFSET),
                buffer.getLong(offset + FIRST_OBSERVATION_OFFSET),
                buffer.getLong(offset + LAST_OBSERVATION_OFFSET),
                buffer.getInt(offset + SESSION_ID_OFFSET),
                buffer.getInt(offset + STREAM_ID_OFFSET),
                channel,
                source);

            final int recordLength = CHANNEL_OFFSET + (SIZE_OF_INT * 2) + channel.length() + source.length();
            offset += BitUtil.align(recordLength, ENTRY_ALIGNMENT);
        }

        return recordsRead;
    }

    /**
     * Consumer function to be implemented by caller of the read method.
     */
    @FunctionalInterface
    public interface EntryConsumer
    {
        void accept(
            long observationCount,
            long totalBytesLost,
            long firstObservationTimestamp,
            long lastObservationTimestamp,
            int sessionId,
            int streamId,
            String channel,
            String source);
    }
}

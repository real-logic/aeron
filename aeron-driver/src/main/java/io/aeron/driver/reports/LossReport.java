/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import static org.agrona.BitUtil.*;

/**
 * A report of loss events on a message stream.
 * <p>
 * The provided {@link AtomicBuffer} can wrap a memory-mapped file so logging can be out of process. This provides
 * the benefit that if a crash or lockup occurs then the log can be read externally without loss of data.
 * <p>
 * <b>Note:</b>This class is NOT threadsafe to be used from multiple logging threads.
 * <p>
 * The error records are recorded to the memory mapped buffer in the following format.
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |R|                    Observation Count                        |
 *  |                                                               |
 *  +-+-------------------------------------------------------------+
 *  |R|                     Total Bytes Lost                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                 First Observation Timestamp                   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                  Last Observation Timestamp                   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                          Session ID                           |
 *  +---------------------------------------------------------------+
 *  |                           Stream ID                           |
 *  +---------------------------------------------------------------+
 *  |                 Channel encoded in US-ASCII                  ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                  Source encoded in US-ASCII                  ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class LossReport
{
    /**
     * Alignment to be applied for each entry offset.
     */
    public static final int ENTRY_ALIGNMENT = CACHE_LINE_LENGTH;

    /**
     * Offset within an entry at which the observation count begins.
     */
    public static final int OBSERVATION_COUNT_OFFSET = 0;

    /**
     * Offset within an entry at which the total bytes field begins.
     */
    public static final int TOTAL_BYTES_LOST_OFFSET = OBSERVATION_COUNT_OFFSET + SIZE_OF_LONG;

    /**
     * Offset within an entry at which the first observation field begins.
     */
    public static final int FIRST_OBSERVATION_OFFSET = TOTAL_BYTES_LOST_OFFSET + SIZE_OF_LONG;

    /**
     * Offset within an entry at which the last observation field begins.
     */
    public static final int LAST_OBSERVATION_OFFSET = FIRST_OBSERVATION_OFFSET + SIZE_OF_LONG;

    /**
     * Offset within an entry at which the session id field begins.
     */
    public static final int SESSION_ID_OFFSET = LAST_OBSERVATION_OFFSET + SIZE_OF_LONG;

    /**
     * Offset within an entry at which the stream id field begins.
     */
    public static final int STREAM_ID_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;

    /**
     * Offset within an entry at which the channel field begins.
     */
    public static final int CHANNEL_OFFSET = STREAM_ID_OFFSET + SIZE_OF_INT;

    private int nextRecordOffset = 0;
    private final AtomicBuffer buffer;

    /**
     * Create a loss report which wraps a buffer which is ideally memory mapped so it can
     * be read from another process.
     *
     * @param buffer to be wrapped.
     */
    public LossReport(final AtomicBuffer buffer)
    {
        buffer.verifyAlignment();
        this.buffer = buffer;
    }

    /**
     * Create a new entry for recording loss on a given stream.
     * <p>
     * If not space is remaining in the error report then null is returned.
     *
     * @param initialBytesLost on the stream.
     * @param timestampMs      at which the first loss was observed.
     * @param sessionId        for the stream.
     * @param streamId         for the stream.
     * @param channel          for the stream.
     * @param source           of the stream.
     * @return a new record or null if the error log has insufficient space.
     */
    public ReportEntry createEntry(
        final long initialBytesLost,
        final long timestampMs,
        final int sessionId,
        final int streamId,
        final String channel,
        final String source)
    {
        ReportEntry reportEntry = null;

        final int requiredCapacity = CHANNEL_OFFSET + (SIZE_OF_INT * 2) + channel.length() + source.length();

        if (requiredCapacity <= (buffer.capacity() - nextRecordOffset))
        {
            final int offset = nextRecordOffset;

            buffer.putLong(offset + TOTAL_BYTES_LOST_OFFSET, initialBytesLost);
            buffer.putLong(offset + FIRST_OBSERVATION_OFFSET, timestampMs);
            buffer.putLong(offset + LAST_OBSERVATION_OFFSET, timestampMs);
            buffer.putInt(offset + SESSION_ID_OFFSET, sessionId);
            buffer.putInt(offset + STREAM_ID_OFFSET, streamId);

            final int encodedChannelLength = buffer.putStringAscii(offset + CHANNEL_OFFSET, channel);
            buffer.putStringAscii(offset + CHANNEL_OFFSET + encodedChannelLength, source);

            buffer.putLongOrdered(offset + OBSERVATION_COUNT_OFFSET, 1);

            reportEntry = new ReportEntry(buffer, offset);
            nextRecordOffset += BitUtil.align(requiredCapacity, ENTRY_ALIGNMENT);
        }

        return reportEntry;
    }

    /**
     * Report entry for a specific stream. Once an entry has been created it can then be used repeatably
     * to capture the aggregate loss on a stream.
     */
    public static class ReportEntry
    {
        private final AtomicBuffer buffer;
        private final int offset;

        ReportEntry(final AtomicBuffer buffer, final int offset)
        {
            this.buffer = buffer;
            this.offset = offset;
        }

        /**
         * Record a loss observation for a particular stream.
         *
         * @param bytesLost   in this observation.
         * @param timestampMs when this observation occurred.
         */
        public void recordObservation(final long bytesLost, final long timestampMs)
        {
            buffer.putLong(offset + LAST_OBSERVATION_OFFSET, timestampMs);
            buffer.getAndAddLong(offset + TOTAL_BYTES_LOST_OFFSET, bytesLost);
            buffer.getAndAddLong(offset + OBSERVATION_COUNT_OFFSET, 1);
        }
    }
}

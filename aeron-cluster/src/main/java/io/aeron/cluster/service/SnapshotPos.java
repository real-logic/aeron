/*
 * Copyright 2016 Real Logic Ltd.
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

import io.aeron.Aeron;
import io.aeron.Counter;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.*;

/**
 * Counter representing the snapshot a service should load on start.
 * <p>
 * Key layout as follows:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |              Log Position at beginning of term                |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |              Message Index at beginning of term               |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |              Timestamp when snapshot was taken                |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class SnapshotPos
{
    /**
     * Type id of a snapshot counter.
     */
    public static final int SNAPSHOT_TYPE_ID = 203;

    /**
     * Represents a null counter id when not found.
     */
    public static final int NULL_COUNTER_ID = -1;

    /**
     * Represents a null value if the counter is not found.
     */
    public static final long NULL_VALUE = -1L;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "snapshot: ";

    public static final int RECORDING_ID_OFFSET = 0;
    public static final int LOG_POSITION_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG;
    public static final int MESSAGE_INDEX_OFFSET = LOG_POSITION_OFFSET + SIZE_OF_LONG;
    public static final int TIMESTAMP_OFFSET = MESSAGE_INDEX_OFFSET + SIZE_OF_LONG;
    public static final int KEY_LENGTH = TIMESTAMP_OFFSET + SIZE_OF_LONG;

    /**
     * Allocate a counter to represent the snapshot services should load on start.
     *
     * @param aeron        to allocate the counter.
     * @param tempBuffer   to use for building the key and label without allocation.
     * @param logPosition  at which the snapshot was taken.
     * @param messageIndex at which the snapshot was taken.
     * @param timestamp    the snapshot was taken.
     * @return the {@link Counter} for the consensus position.
     */
    public static Counter allocate(
        final Aeron aeron,
        final UnsafeBuffer tempBuffer,
        final long logPosition,
        final long messageIndex,
        final long timestamp)
    {
        tempBuffer.putLong(LOG_POSITION_OFFSET, logPosition);
        tempBuffer.putLong(MESSAGE_INDEX_OFFSET, messageIndex);
        tempBuffer.putLong(TIMESTAMP_OFFSET, timestamp);

        int labelOffset = 0;
        labelOffset += tempBuffer.putStringWithoutLengthAscii(KEY_LENGTH + labelOffset, NAME);
        labelOffset += tempBuffer.putLongAscii(KEY_LENGTH + labelOffset, logPosition);
        labelOffset += tempBuffer.putStringWithoutLengthAscii(KEY_LENGTH + labelOffset, " ");
        labelOffset += tempBuffer.putLongAscii(KEY_LENGTH + labelOffset, messageIndex);

        return aeron.addCounter(
            SNAPSHOT_TYPE_ID, tempBuffer, 0, KEY_LENGTH, tempBuffer, KEY_LENGTH, labelOffset);
    }

    /**
     * Find the active counter id for a snapshot.
     *
     * @param countersReader to search within.
     * @return the counter id if found otherwise {@link #NULL_COUNTER_ID}.
     */
    public static int findActiveCounterId(final CountersReader countersReader)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        for (int i = 0, size = countersReader.maxCounterId(); i < size; i++)
        {
            if (countersReader.getCounterState(i) == RECORD_ALLOCATED)
            {
                final int recordOffset = CountersReader.metaDataOffset(i);

                if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == SNAPSHOT_TYPE_ID)
                {
                    return i;
                }
            }
        }

        return NULL_COUNTER_ID;
    }

    /**
     * Get the recording id for the current term.
     *
     * @param countersReader to search within.
     * @param counterId      for the active consensus position.
     * @return the counter id if found otherwise {@link #NULL_VALUE}.
     */
    public static long getRecordingId(final CountersReader countersReader, final int counterId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == SNAPSHOT_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + RECORDING_ID_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the log position for the snapshot.
     *
     * @param countersReader to search within.
     * @param counterId      for the active consensus position.
     * @return the log position if found otherwise {@link #NULL_VALUE}.
     */
    public static long getLogPosition(final CountersReader countersReader, final int counterId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == SNAPSHOT_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + LOG_POSITION_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the message index for the snapshot.
     *
     * @param countersReader to search within.
     * @param counterId      for the active consensus position.
     * @return the message index if found otherwise {@link #NULL_VALUE}.
     */
    public static long getMessageIndex(final CountersReader countersReader, final int counterId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == SNAPSHOT_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + MESSAGE_INDEX_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the timestamp for when the snapshot was taken.
     *
     * @param countersReader to search within.
     * @param counterId      for the active consensus position.
     * @return the timestamp if found otherwise {@link #NULL_VALUE}.
     */
    public static long getTimestamp(final CountersReader countersReader, final int counterId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == SNAPSHOT_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + TIMESTAMP_OFFSET);
            }
        }

        return NULL_VALUE;
    }
}

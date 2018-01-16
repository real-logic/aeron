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
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.*;

/**
 * Counter representing the Recovery state for the cluster.
 * <p>
 * Key layout as follows:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |            Log Position at beginning for snapshot             |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                     Leadership Term ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |              Timestamp at beginning of recovery               |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |               Count of leadership replay terms                |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class RecoveryState
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
    public static final int NULL_VALUE = -1;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "cluster recovery: ";

    public static final int RECORDING_ID_OFFSET = 0;
    public static final int LOG_POSITION_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG;
    public static final int LEADERSHIP_TERM_ID_OFFSET = LOG_POSITION_OFFSET + SIZE_OF_LONG;
    public static final int TIMESTAMP_OFFSET = LEADERSHIP_TERM_ID_OFFSET + SIZE_OF_LONG;
    public static final int REPLAY_TERM_COUNT_OFFSET = TIMESTAMP_OFFSET + SIZE_OF_LONG;
    public static final int KEY_LENGTH = REPLAY_TERM_COUNT_OFFSET + SIZE_OF_INT;

    /**
     * Allocate a counter to represent the snapshot services should load on start.
     *
     * @param aeron           to allocate the counter.
     * @param tempBuffer      to use for building the key and label without allocation.
     * @param logPosition     at which the snapshot was taken.
     * @param leadershipTermId    at which the snapshot was taken.
     * @param timestamp       the snapshot was taken.
     * @param replayTermCount for the count of terms to be replayed during recovery after snapshot.
     * @return the {@link Counter} for the consensus position.
     */
    public static Counter allocate(
        final Aeron aeron,
        final MutableDirectBuffer tempBuffer,
        final long logPosition,
        final long leadershipTermId,
        final long timestamp,
        final int replayTermCount)
    {
        tempBuffer.putLong(LOG_POSITION_OFFSET, logPosition);
        tempBuffer.putLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);
        tempBuffer.putLong(TIMESTAMP_OFFSET, timestamp);
        tempBuffer.putInt(REPLAY_TERM_COUNT_OFFSET, replayTermCount);

        int labelOffset = 0;
        labelOffset += tempBuffer.putStringWithoutLengthAscii(KEY_LENGTH + labelOffset, NAME);
        labelOffset += tempBuffer.putLongAscii(KEY_LENGTH + labelOffset, logPosition);
        labelOffset += tempBuffer.putStringWithoutLengthAscii(KEY_LENGTH + labelOffset, " ");
        labelOffset += tempBuffer.putLongAscii(KEY_LENGTH + labelOffset, leadershipTermId);
        labelOffset += tempBuffer.putStringWithoutLengthAscii(KEY_LENGTH + labelOffset, " ");
        labelOffset += tempBuffer.putIntAscii(KEY_LENGTH + labelOffset, replayTermCount);

        return aeron.addCounter(
            SNAPSHOT_TYPE_ID, tempBuffer, 0, KEY_LENGTH, tempBuffer, KEY_LENGTH, labelOffset);
    }

    /**
     * Find the active counter id for a snapshot.
     *
     * @param counters to search within.
     * @return the counter id if found otherwise {@link #NULL_COUNTER_ID}.
     */
    public static int findCounterId(final CountersReader counters)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        for (int i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            if (counters.getCounterState(i) == RECORD_ALLOCATED)
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
     * Get the recording id for the current leadership term.
     *
     * @param counters  to search within.
     * @param counterId for the active consensus position.
     * @return the counter id if found otherwise {@link #NULL_VALUE}.
     */
    public static long getRecordingId(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
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
     * @param counters  to search within.
     * @param counterId for the active consensus position.
     * @return the log position if found otherwise {@link #NULL_VALUE}.
     */
    public static long getLogPosition(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
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
     * Get the leadership term id for the snapshot.
     *
     * @param counters  to search within.
     * @param counterId for the active consensus position.
     * @return the message index if found otherwise {@link #NULL_VALUE}.
     */
    public static long getLeadershipTermId(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == SNAPSHOT_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + LEADERSHIP_TERM_ID_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the timestamp for when the snapshot was taken.
     *
     * @param counters  to search within.
     * @param counterId for the active consensus position.
     * @return the timestamp if found otherwise {@link #NULL_VALUE}.
     */
    public static long getTimestamp(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == SNAPSHOT_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + TIMESTAMP_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the count of terms that will be replayed during recovery.
     *
     * @param counters  to search within.
     * @param counterId for the active consensus position.
     * @return the count of replay terms if found otherwise {@link #NULL_VALUE}.
     */
    public static int getReplayTermCount(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == SNAPSHOT_TYPE_ID)
            {
                return buffer.getInt(recordOffset + KEY_OFFSET + REPLAY_TERM_COUNT_OFFSET);
            }
        }

        return NULL_VALUE;
    }
}

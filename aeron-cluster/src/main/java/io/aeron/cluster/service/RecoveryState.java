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
 *  |                     Leadership Term ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                 Term position for Snapshot                    |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |              Timestamp at beginning of Recovery               |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |               Count of leadership replay terms                |
 *  +---------------------------------------------------------------+
 *  |                     Count of Services                         |
 *  +---------------------------------------------------------------+
 *  |             Snapshot Recording ID (Service ID 0)              |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |             Snapshot Recording ID (Service ID n)              |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class RecoveryState
{
    /**
     * Type id of a recovery state counter.
     */
    public static final int RECOVERY_STATE_TYPE_ID = 204;

    /**
     * Represents a null value if the counter is not found.
     */
    public static final int NULL_VALUE = -1;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "cluster recovery: leadershipTermId=";

    public static final int LEADERSHIP_TERM_ID_OFFSET = 0;
    public static final int TERM_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + SIZE_OF_LONG;
    public static final int TIMESTAMP_OFFSET = TERM_POSITION_OFFSET + SIZE_OF_LONG;
    public static final int REPLAY_TERM_COUNT_OFFSET = TIMESTAMP_OFFSET + SIZE_OF_LONG;
    public static final int SERVICE_COUNT_OFFSET = REPLAY_TERM_COUNT_OFFSET + SIZE_OF_INT;
    public static final int SNAPSHOT_RECORDING_IDS_OFFSET = SERVICE_COUNT_OFFSET + SIZE_OF_INT;

    /**
     * Allocate a counter to represent the snapshot services should load on start.
     *
     * @param aeron                to allocate the counter.
     * @param tempBuffer           to use for building the key and label without allocation.
     * @param leadershipTermId     at which the snapshot was taken.
     * @param termPosition         at which the snapshot was taken.
     * @param timestamp            the snapshot was taken.
     * @param replayTermCount      for the count of terms to be replayed during recovery after snapshot.
     * @param snapshotRecordingIds for the services to use during recovery indexed by service id.
     * @return the {@link Counter} for the recovery state.
     */
    public static Counter allocate(
        final Aeron aeron,
        final MutableDirectBuffer tempBuffer,
        final long leadershipTermId,
        final long termPosition,
        final long timestamp,
        final int replayTermCount,
        final long... snapshotRecordingIds)
    {
        tempBuffer.putLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);
        tempBuffer.putLong(TERM_POSITION_OFFSET, termPosition);
        tempBuffer.putLong(TIMESTAMP_OFFSET, timestamp);
        tempBuffer.putInt(REPLAY_TERM_COUNT_OFFSET, replayTermCount);

        final int serviceCount = snapshotRecordingIds.length;
        tempBuffer.putInt(SERVICE_COUNT_OFFSET, serviceCount);

        final int keyLength = SNAPSHOT_RECORDING_IDS_OFFSET + (serviceCount * SIZE_OF_LONG);
        final int maxRecordingIdsLength = SNAPSHOT_RECORDING_IDS_OFFSET + (SIZE_OF_LONG * serviceCount);
        if (maxRecordingIdsLength > MAX_KEY_LENGTH)
        {
            throw new IllegalArgumentException(maxRecordingIdsLength + " exceeds max key length " + MAX_KEY_LENGTH);
        }

        for (int i = 0; i < serviceCount; i++)
        {
            tempBuffer.putLong(SNAPSHOT_RECORDING_IDS_OFFSET + (i * SIZE_OF_LONG), snapshotRecordingIds[i]);
        }

        int labelOffset = 0;
        labelOffset += tempBuffer.putStringWithoutLengthAscii(keyLength + labelOffset, NAME);
        labelOffset += tempBuffer.putLongAscii(keyLength + labelOffset, leadershipTermId);
        labelOffset += tempBuffer.putStringWithoutLengthAscii(keyLength + labelOffset, " termPosition=");
        labelOffset += tempBuffer.putLongAscii(keyLength + labelOffset, termPosition);
        labelOffset += tempBuffer.putStringWithoutLengthAscii(keyLength + labelOffset, " replayTermCount=");
        labelOffset += tempBuffer.putIntAscii(keyLength + labelOffset, replayTermCount);

        return aeron.addCounter(
            RECOVERY_STATE_TYPE_ID, tempBuffer, 0, keyLength, tempBuffer, keyLength, labelOffset);
    }

    /**
     * Find the active counter id for recovery state.
     *
     * @param counters to search within.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     */
    public static int findCounterId(final CountersReader counters)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        for (int i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            if (counters.getCounterState(i) == RECORD_ALLOCATED)
            {
                final int recordOffset = CountersReader.metaDataOffset(i);

                if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
                {
                    return i;
                }
            }
        }

        return NULL_COUNTER_ID;
    }

    /**
     * Get the leadership term id for the recovery state.
     *
     * @param counters  to search within.
     * @param counterId for the active recovery counter.
     * @return the leadership term id if found otherwise {@link #NULL_VALUE}.
     */
    public static long getLeadershipTermId(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + LEADERSHIP_TERM_ID_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the term position for recovery within the leadership term.
     *
     * @param counters  to search within.
     * @param counterId for the active recovery counter.
     * @return the term position if found otherwise {@link #NULL_VALUE}.
     */
    public static long getTermPosition(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + TERM_POSITION_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the timestamp at the beginning of recovery.
     *
     * @param counters  to search within.
     * @param counterId for the active recovery counter.
     * @return the timestamp if found otherwise {@link #NULL_VALUE}.
     */
    public static long getTimestamp(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
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
     * @param counterId for the active recovery counter.
     * @return the count of replay terms if found otherwise {@link #NULL_VALUE}.
     */
    public static int getReplayTermCount(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
            {
                return buffer.getInt(recordOffset + KEY_OFFSET + REPLAY_TERM_COUNT_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the recording id of the snapshot for a service.
     *
     * @param counters  to search within.
     * @param counterId for the active recovery counter.
     * @param serviceId for the snapshot required.
     * @return the count of replay terms if found otherwise {@link #NULL_VALUE}.
     */
    public static long getSnapshotRecordingId(final CountersReader counters, final int counterId, final int serviceId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
            {
                final int serviceCount = buffer.getInt(recordOffset + KEY_OFFSET + SERVICE_COUNT_OFFSET);
                if (serviceId < 0 || serviceId >= serviceCount)
                {
                    throw new IllegalArgumentException(
                        "Invalid serviceId " + serviceId + " for count of " + serviceCount);
                }

                return buffer.getLong(
                    recordOffset + KEY_OFFSET + SNAPSHOT_RECORDING_IDS_OFFSET + (serviceId * SIZE_OF_LONG));
            }
        }

        throw new IllegalArgumentException("Active counter not found " + counterId);
    }
}

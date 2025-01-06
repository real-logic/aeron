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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.Counter;
import io.aeron.cluster.client.ClusterException;
import org.agrona.*;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.Aeron.NULL_VALUE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.*;

/**
 * Counter representing the Recovery State for the cluster.
 * <p>
 * Key layout as follows:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                     Leadership Term ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                  Log position for Snapshot                    |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |              Timestamp at beginning of Recovery               |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Cluster ID                            |
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
public final class RecoveryState
{
    /**
     * Type id of a recovery state counter.
     */
    public static final int RECOVERY_STATE_TYPE_ID = AeronCounters.CLUSTER_RECOVERY_STATE_TYPE_ID;

    /**
     * Human-readable name for the counter.
     */
    public static final String NAME = "Cluster recovery: leadershipTermId=";

    /**
     * Offset of the {@code term-id} field.
     */
    public static final int LEADERSHIP_TERM_ID_OFFSET = 0;
    /**
     * Offset of the {@code log-position} field.
     */
    public static final int LOG_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + SIZE_OF_LONG;
    /**
     * Offset of the {@code timestamp} field.
     */
    public static final int TIMESTAMP_OFFSET = LOG_POSITION_OFFSET + SIZE_OF_LONG;
    /**
     * Offset of the {@code cluster-id} field.
     */
    public static final int CLUSTER_ID_OFFSET = TIMESTAMP_OFFSET + SIZE_OF_LONG;
    /**
     * Offset of the {@code service-count} field.
     */
    public static final int SERVICE_COUNT_OFFSET = CLUSTER_ID_OFFSET + SIZE_OF_INT;
    /**
     * Offset of the {@code snapshot-recording-ids} field.
     */
    public static final int SNAPSHOT_RECORDING_IDS_OFFSET = SERVICE_COUNT_OFFSET + SIZE_OF_INT;

    private RecoveryState()
    {
    }

    /**
     * Allocate a counter to represent the snapshot services should load on start.
     *
     * @param aeron                to allocate the counter.
     * @param leadershipTermId     at which the snapshot was taken.
     * @param logPosition          at which the snapshot was taken.
     * @param timestamp            the snapshot was taken.
     * @param clusterId            which identifies the cluster instance.
     * @param snapshotRecordingIds for the services to use during recovery indexed by service id.
     * @return the {@link Counter} for the recovery state.
     */
    public static Counter allocate(
        final Aeron aeron,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final int clusterId,
        final long... snapshotRecordingIds)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);

        buffer.putLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);
        buffer.putLong(LOG_POSITION_OFFSET, logPosition);
        buffer.putLong(TIMESTAMP_OFFSET, timestamp);
        buffer.putInt(CLUSTER_ID_OFFSET, clusterId);

        final int serviceCount = snapshotRecordingIds.length;
        buffer.putInt(SERVICE_COUNT_OFFSET, serviceCount);

        final int keyLength = SNAPSHOT_RECORDING_IDS_OFFSET + (serviceCount * SIZE_OF_LONG);
        if (keyLength > MAX_KEY_LENGTH)
        {
            throw new ClusterException(keyLength + " exceeds max key length " + MAX_KEY_LENGTH);
        }

        for (int i = 0; i < serviceCount; i++)
        {
            buffer.putLong(SNAPSHOT_RECORDING_IDS_OFFSET + (i * SIZE_OF_LONG), snapshotRecordingIds[i]);
        }

        final int labelOffset = BitUtil.align(keyLength, SIZE_OF_INT);
        int labelLength = 0;
        labelLength += buffer.putStringWithoutLengthAscii(labelOffset + labelLength, NAME);
        labelLength += buffer.putLongAscii(keyLength + labelLength, leadershipTermId);
        labelLength += buffer.putStringWithoutLengthAscii(labelOffset + labelLength, " logPosition=");
        labelLength += buffer.putLongAscii(labelOffset + labelLength, logPosition);
        labelLength += buffer.putStringWithoutLengthAscii(labelOffset + labelLength, " clusterId=");
        labelLength += buffer.putIntAscii(labelOffset + labelLength, clusterId);

        return aeron.addCounter(RECOVERY_STATE_TYPE_ID, buffer, 0, keyLength, buffer, labelOffset, labelLength);
    }

    /**
     * Find the active counter id for recovery state.
     *
     * @param counters  to search within.
     * @param clusterId to constrain the search.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     */
    public static int findCounterId(final CountersReader counters, final int clusterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        for (int i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            final int counterState = counters.getCounterState(i);
            if (counterState == RECORD_ALLOCATED && counters.getCounterTypeId(i) == RECOVERY_STATE_TYPE_ID)
            {
                if (buffer.getInt(CountersReader.metaDataOffset(i) + KEY_OFFSET + CLUSTER_ID_OFFSET) == clusterId)
                {
                    return i;
                }
            }
            else if (RECORD_UNUSED == counterState)
            {
                break;
            }
        }

        return NULL_COUNTER_ID;
    }

    /**
     * Get the leadership term id for the snapshot state. {@link Aeron#NULL_VALUE} if no snapshot for recovery.
     *
     * @param counters  to search within.
     * @param counterId for the active recovery counter.
     * @return the leadership term id if found otherwise {@link Aeron#NULL_VALUE}.
     */
    public static long getLeadershipTermId(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED &&
            counters.getCounterTypeId(counterId) == RECOVERY_STATE_TYPE_ID)
        {
            return buffer.getLong(CountersReader.metaDataOffset(counterId) + KEY_OFFSET + LEADERSHIP_TERM_ID_OFFSET);
        }

        return NULL_VALUE;
    }

    /**
     * Get the position at which the snapshot was taken. {@link Aeron#NULL_VALUE} if no snapshot for recovery.
     *
     * @param counters  to search within.
     * @param counterId for the active recovery counter.
     * @return the log position if found otherwise {@link Aeron#NULL_VALUE}.
     */
    public static long getLogPosition(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED &&
            counters.getCounterTypeId(counterId) == RECOVERY_STATE_TYPE_ID)
        {
            return buffer.getLong(CountersReader.metaDataOffset(counterId) + KEY_OFFSET + LOG_POSITION_OFFSET);
        }

        return NULL_VALUE;
    }

    /**
     * Get the timestamp at the beginning of recovery. {@link Aeron#NULL_VALUE} if no snapshot for recovery.
     *
     * @param counters  to search within.
     * @param counterId for the active recovery counter.
     * @return the timestamp if found otherwise {@link Aeron#NULL_VALUE}.
     */
    public static long getTimestamp(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED &&
            counters.getCounterTypeId(counterId) == RECOVERY_STATE_TYPE_ID)
        {
            return buffer.getLong(CountersReader.metaDataOffset(counterId) + KEY_OFFSET + TIMESTAMP_OFFSET);
        }

        return NULL_VALUE;
    }

    /**
     * Get the recording id of the snapshot for a service.
     *
     * @param counters  to search within.
     * @param counterId for the active recovery counter.
     * @param serviceId for the snapshot required.
     * @return the count of replay terms if found otherwise {@link Aeron#NULL_VALUE}.
     */
    public static long getSnapshotRecordingId(final CountersReader counters, final int counterId, final int serviceId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED &&
            counters.getCounterTypeId(counterId) == RECOVERY_STATE_TYPE_ID)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            final int serviceCount = buffer.getInt(recordOffset + KEY_OFFSET + SERVICE_COUNT_OFFSET);
            if (serviceId < 0 || serviceId >= serviceCount)
            {
                throw new ClusterException("invalid serviceId " + serviceId + " for count of " + serviceCount);
            }

            return buffer.getLong(
                recordOffset + KEY_OFFSET + SNAPSHOT_RECORDING_IDS_OFFSET + (serviceId * SIZE_OF_LONG));
        }

        throw new ClusterException("active counter not found " + counterId);
    }
}

/*
 * Copyright 2014-2022 Real Logic Limited.
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

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterException;
import io.aeron.exceptions.AeronException;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

final class LogReplication
{
    private final long replicationId;
    private final long stopPosition;
    private final long progressCheckTimeoutNs;
    private final long progressCheckIntervalNs;

    private int recordingPositionCounterId = NULL_COUNTER_ID;
    private long recordingId;
    private long position = NULL_POSITION;

    private long progressDeadlineNs;
    private long progressCheckDeadlineNs;
    private final AeronArchive archive;
    private RecordingSignal lastRecordingSignal = RecordingSignal.NULL_VAL;

    private boolean isStopped = false;

    LogReplication(
        final AeronArchive archive,
        final long srcRecordingId,
        final long dstRecordingId,
        final long stopPosition,
        final String srcArchiveChannel,
        final String replicationChannel,
        final long progressCheckTimeoutNs,
        final long progressCheckIntervalNs,
        final long nowNs)
    {
        this.archive = archive;
        this.stopPosition = stopPosition;
        this.progressCheckTimeoutNs = progressCheckTimeoutNs;
        this.progressCheckIntervalNs = progressCheckIntervalNs;
        this.progressDeadlineNs = nowNs + progressCheckTimeoutNs;
        this.progressCheckDeadlineNs = nowNs + progressCheckIntervalNs;

        replicationId = archive.replicate(
            srcRecordingId,
            dstRecordingId,
            stopPosition,
            archive.context().controlRequestStreamId(),
            srcArchiveChannel,
            null,
            replicationChannel);
    }

    boolean isDone(final long nowNs)
    {
        if (position == stopPosition && isStopped)
        {
            return true;
        }

        if (position > stopPosition)
        {
            throw new ClusterException("log replication has progressed past stopPosition: " + this);
        }

        if (nowNs >= progressCheckDeadlineNs)
        {
            progressCheckDeadlineNs = nowNs + progressCheckIntervalNs;

            if (NULL_COUNTER_ID != recordingPositionCounterId)
            {
                final CountersReader counters = archive.context().aeron().countersReader();
                final long recordingPosition = counters.getCounterValue(recordingPositionCounterId);

                if (RecordingPos.isActive(counters, recordingPositionCounterId, recordingId) &&
                    recordingPosition > position)
                {
                    position = recordingPosition;
                    progressDeadlineNs = nowNs + progressCheckTimeoutNs;
                }
            }
        }

        if (nowNs >= progressDeadlineNs)
        {
            if (position < stopPosition)
            {
                throw new ClusterException("log replication has not progressed: " + this, AeronException.Category.WARN);
            }
            else
            {
                throw new ClusterException("log replication failed to stop: " + this);
            }
        }

        return false;
    }

    long position()
    {
        return position;
    }

    long recordingId()
    {
        return recordingId;
    }

    void close()
    {
        if (!isStopped)
        {
            try
            {
                isStopped = true;
                archive.tryStopReplication(replicationId);
            }
            catch (final Exception ex)
            {
                throw new ClusterException("failed to stop log replication", ex, AeronException.Category.WARN);
            }
        }
    }

    void onSignal(final long correlationId, final long recordingId, final long position, final RecordingSignal signal)
    {
        if (correlationId == replicationId)
        {
            switch (signal)
            {
                case EXTEND:
                    final CountersReader counters = archive.context().aeron().countersReader();
                    recordingPositionCounterId = RecordingPos.findCounterIdByRecording(counters, recordingId);
                    break;

                case DELETE:
                    throw new ClusterException("recording was deleted during replication: " + this);

                case REPLICATE_END:
                    isStopped = true;
                    break;
            }

            this.recordingId = recordingId;
            this.lastRecordingSignal = signal;

            if (NULL_POSITION != position)
            {
                this.position = position;
            }
        }
    }

    public String toString()
    {
        return "LogReplication{" +
            "replicationId=" + replicationId +
            ", recordingPositionCounterId=" + recordingPositionCounterId +
            ", recordingId=" + recordingId +
            ", position=" + position +
            ", stopPosition=" + stopPosition +
            ", stopped=" + isStopped +
            ", lastRecordingSignal=" + lastRecordingSignal +
            ", progressDeadlineNs=" + progressDeadlineNs +
            ", progressCheckDeadlineNs=" + progressCheckDeadlineNs +
            ", progressCheckTimeoutNs=" + progressCheckTimeoutNs +
            ", progressCheckIntervalNs=" + progressCheckIntervalNs +
            '}';
    }
}

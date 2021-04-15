/*
 * Copyright 2014-2021 Real Logic Limited.
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
        final String srcArchiveEndpoint,
        final String logReplicationChannel,
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

        final String srcArchiveChannel = "aeron:udp?endpoint=" + srcArchiveEndpoint;

        final int srcControlStreamId = archive.context().controlRequestStreamId();
        replicationId = archive.replicate(
            srcRecordingId,
            dstRecordingId,
            stopPosition,
            srcControlStreamId,
            srcArchiveChannel,
            null,
            logReplicationChannel);
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
                throw new ClusterException("log replication has not progressed: " + this);
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
                archive.tryStopReplication(replicationId);
                isStopped = true;
            }
            catch (final Exception e)
            {
                throw new ClusterException("failed to stop log replication", e);
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
                case STOP:
                    this.isStopped = true;
                    break;
            }

            this.recordingId = recordingId;
            this.position = position;
            this.lastRecordingSignal = signal;
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

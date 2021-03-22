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

import io.aeron.archive.client.*;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterException;
import io.aeron.status.ReadableCounter;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

final class LogReplication
{
    private final long replicationId;
    private final long stopPosition;
    private final long progressCheckTimeoutNs;
    private final long progressCheckIntervalNs;

    private long recordingId;
    private long position = AeronArchive.NULL_POSITION;

    private long progressDeadlineNs;
    private long progressCheckDeadlineNs;
    private final AeronArchive archive;
    private ReadableCounter recordingPosition = null;
    private RecordingSignal lastRecordingSignal = RecordingSignal.NULL_VAL;

    private boolean isClosed = false;

    LogReplication(
        final AeronArchive archive,
        final long srcRecordingId,
        final long dstRecordingId,
        final long stopPosition,
        final int srcArchiveStreamId,
        final String srcArchiveEndpoint,
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

        replicationId = archive.replicate(
            srcRecordingId, dstRecordingId, stopPosition, srcArchiveStreamId, srcArchiveChannel, null, null);
    }

    boolean isDone(final long nowNs)
    {
        if (position == stopPosition)
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

            if (null != recordingPosition)
            {
                final long recordingPosition = this.recordingPosition.get();
                if (recordingPosition > position)
                {
                    position = recordingPosition;
                    progressDeadlineNs = nowNs + progressCheckTimeoutNs;
                }
            }
        }

        if (nowNs >= progressDeadlineNs)
        {
            throw new ClusterException("log replication has not progressed: " + this);
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

    boolean isClosed()
    {
        return isClosed;
    }

    void close()
    {
        if (isClosed)
        {
            return;
        }

        if (RecordingSignal.STOP != lastRecordingSignal)
        {
            try
            {
                archive.stopReplication(replicationId);
            }
            catch (final Exception ignore)
            {
            }
        }

        isClosed = true;
    }

    void onSignal(final long correlationId, final long recordingId, final long position, final RecordingSignal signal)
    {
        if (correlationId == replicationId)
        {
            if (RecordingSignal.EXTEND == signal)
            {
                recordingPosition = newRecordingPosition(recordingId);
            }
            else if (RecordingSignal.STOP == signal)
            {
                recordingPosition = null;
            }
            else if (RecordingSignal.DELETE == signal)
            {
                throw new ClusterException("recording was deleted during replication, state=" + this);
            }

            this.recordingId = recordingId;
            this.lastRecordingSignal = signal;
            this.position = position;
        }
    }

    ReadableCounter newRecordingPosition(final long recordingId)
    {
        final CountersReader counters = archive.context().aeron().countersReader();
        final int counterId = RecordingPos.findCounterIdByRecording(counters, recordingId);
        if (NULL_COUNTER_ID == counterId)
        {
            throw new ClusterException("no recording counter found for recordingId=" + recordingId);
        }

        return new ReadableCounter(counters, counterId);
    }

    public String toString()
    {
        return "LogReplication{" +
            "replicationId=" + replicationId +
            ", recordingId=" + recordingId +
            ", position=" + position +
            ", stopPosition=" + stopPosition +
            ", lastRecordingSignal=" + lastRecordingSignal +
            ", progressDeadlineNs=" + progressDeadlineNs +
            ", progressCheckDeadlineNs=" + progressCheckDeadlineNs +
            ", progressCheckTimeoutNs=" + progressCheckTimeoutNs +
            ", progressCheckIntervalNs=" + progressCheckIntervalNs +
            '}';
    }
}

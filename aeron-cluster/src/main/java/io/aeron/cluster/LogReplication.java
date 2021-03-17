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

import io.aeron.Aeron;
import io.aeron.archive.client.*;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.cluster.client.ClusterException;

final class LogReplication
{
    private final long replicationId;
    private final AeronArchive archive;
    private final long stopPosition;
    private final long progressCheckTimeoutNs;
    private final long progressCheckIntervalNs;

    private long recordingId;
    private long position = AeronArchive.NULL_POSITION;

    private long nextProgressCheckDeadlineNs = Aeron.NULL_VALUE;
    private long progressTimeoutDeadlineNs = Aeron.NULL_VALUE;
    private RecordingSignal lastRecordingSignal = RecordingSignal.NULL_VAL;

    private boolean isClosed = false;

    LogReplication(
        final AeronArchive archive,
        final long srcRecordingId,
        final long dstRecordingId,
        final int srcArchiveStreamId,
        final String srcArchiveEndpoint,
        final long stopPosition,
        final long progressCheckTimeoutNs,
        final long progressCheckIntervalNs)
    {
        this.archive = archive;
        this.stopPosition = stopPosition;
        this.progressCheckTimeoutNs = progressCheckTimeoutNs;
        this.progressCheckIntervalNs = progressCheckIntervalNs;

        final String srcArchiveChannel = "aeron:udp?endpoint=" + srcArchiveEndpoint;

        replicationId = archive.replicate(
            srcRecordingId, dstRecordingId, srcArchiveStreamId, srcArchiveChannel, null, stopPosition);
    }

    boolean isDone(final long nowNs)
    {
        if (position == stopPosition)
        {
            return true;
        }

        if (position > stopPosition)
        {
            throw new ClusterException(
                "Log replication has progressed too far, position > stopPosition, state=" + this);
        }

        if (Aeron.NULL_VALUE == progressTimeoutDeadlineNs)
        {
            progressTimeoutDeadlineNs = nowNs + progressCheckTimeoutNs;
        }

        if (Aeron.NULL_VALUE == nextProgressCheckDeadlineNs)
        {
            nextProgressCheckDeadlineNs = nowNs + progressCheckIntervalNs;
        }

        if (Aeron.NULL_VALUE != recordingId && nowNs >= nextProgressCheckDeadlineNs)
        {
            final long recordingPosition = archive.getRecordingPosition(recordingId);
            if (recordingPosition > position)
            {
                position = recordingPosition;
                progressTimeoutDeadlineNs = nowNs + progressCheckTimeoutNs;
            }

            nextProgressCheckDeadlineNs = nowNs + progressCheckIntervalNs;
        }

        if (nowNs >= progressTimeoutDeadlineNs)
        {
            throw new ClusterException(
                "Log Replicate has had no progress for " + progressCheckTimeoutNs + "ns, state: " + this);
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

    long progressCheckTimeoutNs()
    {
        return progressCheckTimeoutNs;
    }

    long progressCheckIntervalNs()
    {
        return progressCheckIntervalNs;
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
            if (RecordingSignal.DELETE == signal)
            {
                throw new ClusterException("Recording was deleted during replication, state=" + this);
            }

            this.recordingId = recordingId;
            this.lastRecordingSignal = signal;
            this.position = position;
        }
    }

    public String toString()
    {
        return "LogReplication{" +
            "replicationId=" + replicationId +
            ", recordingId=" + recordingId +
            ", position=" + position +
            ", stopPosition=" + stopPosition +
            ", lastRecordingSignal=" + lastRecordingSignal +
            ", nextProgressCheckDeadlineNs=" + nextProgressCheckDeadlineNs +
            ", progressTimeoutDeadlineNs=" + progressTimeoutDeadlineNs +
            ", progressCheckTimeoutNs=" + progressCheckTimeoutNs +
            ", progressCheckIntervalNs=" + progressCheckIntervalNs +
            '}';
    }
}

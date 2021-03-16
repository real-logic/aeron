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
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingSignal;

import static io.aeron.cluster.ConsensusModuleSnapshotLoader.FRAGMENT_LIMIT;

final class LogReplication implements ControlEventListener, RecordingSignalConsumer
{
    private final long replicationId;
    private final AeronArchive archive;
    private final RecordingSignalAdapter recordingSignalAdapter;

    private boolean isDone = false;
    private long position = AeronArchive.NULL_POSITION;
    private long recordingId;

    LogReplication(
        final AeronArchive archive,
        final long srcRecordingId,
        final long dstRecordingId,
        final int srcArchiveStreamId,
        final String srcArchiveEndpoint,
        final long stopPosition)
    {
        this.archive = archive;

        final String srcArchiveChannel = "aeron:udp?endpoint=" + srcArchiveEndpoint;

        replicationId = archive.replicate(
            srcRecordingId, dstRecordingId, srcArchiveStreamId, srcArchiveChannel, null, stopPosition);

        recordingSignalAdapter = new RecordingSignalAdapter(
            archive.controlSessionId(), this, this, archive.controlResponsePoller().subscription(), FRAGMENT_LIMIT);
    }

    boolean isDone()
    {
        return isDone;
    }

    int doWork()
    {
        return recordingSignalAdapter.poll();
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
        if (!isDone)
        {
            try
            {
                archive.stopReplication(replicationId);
            }
            catch (final Exception ignore)
            {
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onResponse(
        final long controlSessionId,
        final long correlationId,
        final long relevantId,
        final ControlResponseCode code,
        final String errorMessage)
    {
        if (ControlResponseCode.ERROR == code)
        {
            throw new ArchiveException(errorMessage, (int)relevantId, correlationId);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onSignal(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long subscriptionId,
        final long position,
        final RecordingSignal signal)
    {

        if (correlationId == replicationId && RecordingSignal.STOP == signal)
        {
            this.position = position;
            this.recordingId = recordingId;
            isDone = true;
        }
    }
}

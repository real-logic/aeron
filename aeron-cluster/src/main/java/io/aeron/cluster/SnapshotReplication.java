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

final class SnapshotReplication
{
    private final long replicationId;
    private long recordingId = RecordingPos.NULL_RECORDING_ID;
    private boolean isDone = false;
    private boolean hasSync = false;
    private final boolean isNew;

    SnapshotReplication(final long replicationId, final boolean isNew)
    {
        this.replicationId = replicationId;
        this.isNew = isNew;
    }

    void close(final AeronArchive archive)
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

    long recordingId()
    {
        return recordingId;
    }

    boolean isComplete()
    {
        return hasSync;
    }

    boolean isDone()
    {
        return isDone;
    }

    void onSignal(final long correlationId, final long recordingId, final long position, final RecordingSignal signal)
    {
        if (correlationId == replicationId)
        {
            if (RecordingPos.NULL_RECORDING_ID != recordingId)
            {
                this.recordingId = recordingId;
            }

            if (RecordingSignal.EXTEND == signal)
            {
                if (isNew && 0 != position)
                {
                    throw new ClusterException("expected new extend signal at 0: position=" + position);
                }
            }
            else if (RecordingSignal.SYNC == signal)
            {
                hasSync = true;
            }
            else if (RecordingSignal.REPLICATE_END == signal)
            {
                isDone = true;
            }
        }
    }
}

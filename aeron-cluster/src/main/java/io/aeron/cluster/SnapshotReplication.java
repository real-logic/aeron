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

final class SnapshotReplication
{
    private final long replicationId;
    private long recordingId = RecordingPos.NULL_RECORDING_ID;
    private boolean isDone = false;
    private boolean isSync = false;
    private String errorMessage;

    SnapshotReplication(final long replicationId)
    {
        this.replicationId = replicationId;
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

    boolean isDone()
    {
        return isDone;
    }

    void checkForError()
    {
        if (null != errorMessage)
        {
            throw new ClusterException("error occurred while retrieving snapshot: " + errorMessage);
        }
    }

    void onSignal(final long correlationId, final long recordingId, final long position, final RecordingSignal signal)
    {
        if (correlationId == replicationId)
        {
            if (RecordingSignal.EXTEND == signal)
            {
                if (0 != position)
                {
                    errorMessage = "unexpected start position expected=0 actual=" + position;
                }
                else
                {
                    this.recordingId = recordingId;
                }
            }
            else if (RecordingSignal.SYNC == signal)
            {
                isSync = true;
            }
            else if (RecordingSignal.STOP == signal)
            {
                if (isSync)
                {
                    isDone = true;
                }
                else
                {
                    errorMessage = "unexpected stop position " + position;
                }
            }
        }
    }
}

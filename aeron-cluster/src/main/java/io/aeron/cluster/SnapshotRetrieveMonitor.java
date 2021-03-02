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
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterException;

import static io.aeron.cluster.ConsensusModuleSnapshotLoader.FRAGMENT_LIMIT;

final class SnapshotRetrieveMonitor implements ControlEventListener, RecordingSignalConsumer
{
    private final RecordingSignalAdapter recordingSignalAdapter;

    private long recordingId = RecordingPos.NULL_RECORDING_ID;
    private boolean isDone = false;
    private boolean isSync = false;
    private String errorMessage;

    SnapshotRetrieveMonitor(final AeronArchive archive)
    {
        recordingSignalAdapter = new RecordingSignalAdapter(
            archive.controlSessionId(), this, this, archive.controlResponsePoller().subscription(), FRAGMENT_LIMIT);
    }

    long recordingId()
    {
        return recordingId;
    }

    boolean isDone()
    {
        return isDone;
    }

    int poll()
    {
        final int fragments = recordingSignalAdapter.poll();
        if (null != errorMessage)
        {
            throw new ClusterException("error occurred while retrieving snapshot: " + errorMessage);
        }

        return fragments;
    }

    public void onResponse(
        final long controlSessionId,
        final long correlationId,
        final long relevantId,
        final ControlResponseCode code,
        final String errorMessage)
    {
        if (ControlResponseCode.ERROR == code)
        {
            this.errorMessage = errorMessage;
        }
    }

    public void onSignal(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long subscriptionId,
        final long position,
        final RecordingSignal signal)
    {
        if ((RecordingSignal.START == signal || RecordingSignal.EXTEND == signal) &&
            RecordingPos.NULL_RECORDING_ID == this.recordingId)
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
        else if (RecordingSignal.SYNC == signal && recordingId == this.recordingId)
        {
            isSync = true;
        }
        else if (RecordingSignal.STOP == signal && recordingId == this.recordingId)
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

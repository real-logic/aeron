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
package io.aeron.cluster;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.status.CountersReader;

public class RecordingCatchUp
{
    private final AeronArchive targetArchive;
    private final AeronArchive replayArchive;
    private final CountersReader countersReader;
    private final long caughtUpPosition;
    private final long replaySessionId;
    private final int counterId;

    private final String replayChannel;
    private final int replayStreamId;

    public RecordingCatchUp(
        final AeronArchive targetArchive,
        final CountersReader targetCounters,
        final long recordingIdToExtend,
        final AeronArchive replayArchive,
        final long recordingIdToReplay,
        final long fromPosition,
        final long toPosition,
        final String replayChannel,
        final int replayStreamId)
    {
        this.targetArchive = targetArchive;
        this.replayArchive = replayArchive;
        this.countersReader = targetCounters;
        this.caughtUpPosition = toPosition;
        this.replayChannel = replayChannel;
        this.replayStreamId = replayStreamId;

        targetArchive.extendRecording(recordingIdToExtend, replayChannel, replayStreamId, SourceLocation.REMOTE);

        replaySessionId = replayArchive.startReplay(
            recordingIdToReplay, fromPosition, toPosition - fromPosition, replayChannel, replayStreamId);

        // TODO: loop waiting to find
        counterId = RecordingPos.findCounterIdByRecording(targetCounters, recordingIdToExtend);
    }

    public void cancel()
    {
        replayArchive.stopReplay(replaySessionId);
        targetArchive.stopRecording(replayChannel, replayStreamId);
    }

    public boolean isCaughtUp()
    {
        return currentPosition() >= caughtUpPosition;
    }

    public boolean isPossiblyStalled(final long stallTimeoutNs)
    {
        return false;
    }

    public long currentPosition()
    {
        return countersReader.getCounterValue(counterId);
    }

    public static boolean foundRecordingIdFromRemoteArchive(
        final AeronArchive remoteArchive,
        final String recordingChannel,
        final int recordingStreamId,
        final MutableLong remoteRecordingId,
        final MutableLong remoteStopPosition)
    {
        final RecordingDescriptorConsumer consumer =
            (controlSessionId,
            correlationId,
            recordingId,
            startTimestamp,
            stopTimestamp,
            startPosition,
            stopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) ->
            {
                remoteRecordingId.value = recordingId;
                remoteStopPosition.value = stopPosition;
            };

        return remoteArchive.listRecordingsForUri(0, 1, recordingChannel, recordingStreamId, consumer) > 0;
    }
}

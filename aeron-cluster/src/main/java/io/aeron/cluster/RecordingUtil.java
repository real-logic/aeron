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
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.CountersReader;

public class RecordingUtil
{
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

    public static void extendRecordingViaRemoteReplay(
        final AeronArchive targetArchive,
        final CountersReader targetCounters,
        final long recordingIdToExtend,
        final AeronArchive replayArchive,
        final long recordingIdToReplay,
        final long fromPosition,
        final long toPosition,
        final String replayChannel,
        final int replayStreamId,
        final NanoClock nanoClock,
        final long stallTimeoutNs)
    {
        targetArchive.extendRecording(recordingIdToExtend, replayChannel, replayStreamId, SourceLocation.REMOTE);

        final long replaySessionId = replayArchive.startReplay(
            recordingIdToReplay, fromPosition, toPosition - fromPosition, replayChannel, replayStreamId);

        final int counterId = RecordingPos.findCounterIdByRecording(targetCounters, recordingIdToExtend);

        long recordingPosition, previousRecordingPosition = fromPosition;
        long stallDeadlineNs = nanoClock.nanoTime() + stallTimeoutNs;

        while ((recordingPosition = targetCounters.getCounterValue(counterId)) < toPosition)
        {
            final long nowNs = nanoClock.nanoTime();

            if (recordingPosition == previousRecordingPosition)
            {
                if (nowNs > stallDeadlineNs)
                {
                    replayArchive.stopReplay(replaySessionId);
                    targetArchive.stopRecording(replayChannel, replayStreamId);

                    throw new IllegalStateException("extending of recording via remote replay stalled at position " +
                        previousRecordingPosition + " for " + stallDeadlineNs + " nanoseconds");
                }
            }
            else
            {
                previousRecordingPosition = recordingPosition;
                stallDeadlineNs = nowNs + stallDeadlineNs;
            }

            Thread.yield();
        }
    }
}

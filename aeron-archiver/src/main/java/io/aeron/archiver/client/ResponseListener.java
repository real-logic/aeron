/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron.archiver.client;

import io.aeron.archiver.codecs.ControlResponseCode;

/**
 * Interface for listening to events from the archiver in response to requests.
 */
public interface ResponseListener
{
    /**
     * An event has been received from the Archive in response to a request with a given correlation id.
     *
     * @param correlationId of the associated request.
     * @param code          for the response status.
     * @param errorMessage  when is set if the response code is not OK.
     */
    void onResponse(long correlationId, ControlResponseCode code, String errorMessage);

    /**
     * Notifies the successful start of a recording replay.
     *
     * @param correlationId of the associated request to start a replay.
     * @param replayId      for the recording that is being replayed.
     */
    void onReplayStarted(long correlationId, long replayId);

    /**
     * Notifies the successful abort of recording replay.
     *
     * @param correlationId for the associated request to abort the replay.
     * @param endPosition   reached for the replay.
     */
    void onReplayAborted(long correlationId, long endPosition);

    /**
     * A recording descriptor returned as a result of requesting a listing of recordings.
     *
     * @param correlationId     of the associated request to list recordings.
     * @param recordingId       of this recording descriptor.
     * @param joiningTimestamp  for the recording.
     * @param endTimestamp      for the recording.
     * @param joiningPosition   for the recording against the recorded publication.
     * @param endPosition       reached for the recording.
     * @param initialTermId     for the recorded publication.
     * @param termBufferLength  for the recorded publication.
     * @param mtuLength         for the recorded publication.
     * @param segmentFileLength for the recording which is a multiple of termBufferLength.
     * @param sessionId         for the recorded publication.
     * @param streamId          for the recorded publication.
     * @param channel           for the recorded publication.
     * @param sourceIdentity    for the recorded publication.
     */
    void onRecordingDescriptor(
        long correlationId,
        long recordingId,
        long joiningTimestamp,
        long endTimestamp,
        long joiningPosition,
        long endPosition,
        int initialTermId,
        int termBufferLength,
        int mtuLength,
        int segmentFileLength,
        int sessionId,
        int streamId,
        String channel,
        String sourceIdentity);

    /**
     * Notifies that the request for a recording descriptor of given id has not been found.
     *
     * @param correlationId  of the associated request to replay a recording.
     * @param recordingId    requested for recording descriptor.
     * @param maxRecordingId for this archive.
     */
    void onRecordingNotFound(long correlationId, long recordingId, long maxRecordingId);
}

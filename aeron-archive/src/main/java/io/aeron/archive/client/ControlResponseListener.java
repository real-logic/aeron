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
package io.aeron.archive.client;

import io.aeron.archive.codecs.ControlResponseCode;

/**
 * Interface for listening to events from the archive in response to requests.
 */
public interface ControlResponseListener extends RecordingDescriptorConsumer
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
     * @param stopPosition   reached for the replay.
     */
    void onReplayAborted(long correlationId, long stopPosition);

    /**
     * Notifies that the request for a recording descriptor of given id is not found
     *
     * @param correlationId  of the associated request to replay a recording.
     * @param recordingId    requested for recording descriptor.
     * @param maxRecordingId for this archive.
     */
    void onUnknownRecording(long correlationId, long recordingId, long maxRecordingId);
}

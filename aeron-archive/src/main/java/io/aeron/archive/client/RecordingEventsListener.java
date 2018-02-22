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
package io.aeron.archive.client;

/**
 * Event listener for observing the status of recordings for an Archive.
 */
public interface RecordingEventsListener
{
    /**
     * Fired when a recording is started.
     *
     * @param recordingId    assigned to the new recording.
     * @param startPosition  in the stream at which the recording started.
     * @param sessionId      of the publication being recorded.
     * @param streamId       of the publication being recorded.
     * @param channel        of the publication being recorded.
     * @param sourceIdentity of the publication being recorded.
     */
    void onStart(
        long recordingId,
        long startPosition,
        int sessionId,
        int streamId,
        String channel,
        String sourceIdentity);

    /**
     * Progress indication of an active recording.
     *
     * @param recordingId   for which progress is being reported.
     * @param startPosition in the stream at which the recording started.
     * @param position      reached in recording the publication.
     */
    void onProgress(long recordingId, long startPosition, long position);

    /**
     * Fired when a recording is stopped.
     *
     * @param recordingId   of the publication that has stopped recording.
     * @param startPosition in the stream at which the recording started.
     * @param stopPosition  at which the recording stopped.
     */
    void onStop(long recordingId, long startPosition, long stopPosition);
}

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

public interface ResponseListener
{
    void onResponse(
        ControlResponseCode code,
        String errorMessage,
        long correlationId);

    void onReplayStarted(
        long replayId,
        long correlationId);

    void onReplayAborted(
        long lastPosition,
        long correlationId);

    void onRecordingDescriptor(
        long recordingId,
        int segmentFileLength,
        int termBufferLength,
        long startTime,
        long joiningPosition,
        long endTime,
        long lastPosition,
        String source,
        int sessionId,
        String channel,
        int streamId,
        long correlationId);

    void onRecordingNotFound(
        long recordingId,
        long maxRecordingId,
        long correlationId);
}

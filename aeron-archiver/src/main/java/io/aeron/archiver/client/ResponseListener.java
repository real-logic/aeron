/*
 * Copyright 2014 - 2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archiver.client;

public interface ResponseListener
{
    void onResponse(
        String err,
        int correlationId);

    void onReplayStarted(
        int replayId,
        int correlationId);

    void onReplayAborted(
        int lastTermId,
        int lastTermOffset,
        int correlationId);

    void onRecordingDescriptor(
        int recordingId,
        int segmentFileLength,
        int termBufferLength,
        long startTime,
        int initialTermId,
        int initialTermOffset,
        long endTime,
        int lastTermId,
        int lastTermOffset,
        String source,
        int sessionId,
        String channel,
        int streamId,
        int correlationId);

    void onRecordingNotFound(
        int recordingId,
        int maxRecordingId,
        int correlationId);
}

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
package io.aeron.archive;

import io.aeron.archive.codecs.SourceLocation;

/**
 * Listener to control request messages from archive clients.
 */
interface ControlRequestListener
{
    void onConnect(long correlationId, String channel, int streamId);

    void onCloseSession(long controlSessionId);

    void onStartRecording(
        long controlSessionId,
        long correlationId,
        String channel,
        int streamId,
        SourceLocation sourceLocation);

    void onStopRecording(long controlSessionId, long correlationId, String channel, int streamId);

    void onStartReplay(
        long controlSessionId,
        long correlationId,
        int replayStreamId,
        String replayChannel,
        long recordingId,
        long position,
        long length);

    void onListRecordings(long controlSessionId, long correlationId, long fromRecordingId, int recordCount);

    void onListRecordingsForUri(
        long controlSessionId,
        long correlationId,
        long fromRecordingId,
        int recordCount,
        String channel,
        int streamId);
}

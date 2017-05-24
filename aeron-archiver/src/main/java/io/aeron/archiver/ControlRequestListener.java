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
package io.aeron.archiver;

interface ControlRequestListener
{
    void onConnect(String channel, int streamId);

    void onStopRecording(long correlationId, long recordingId);

    void onStartRecording(long correlationId, String channel, int streamId);

    void onListRecordings(long correlationId, long fromRecordingId, int recordCount);

    void onAbortReplay(long correlationId, long replyId);

    void onStartReplay(
        long correlationId,
        int replayStreamId,
        String replayChannel,
        long recordingId,
        long position,
        long length);
}

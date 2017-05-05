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
package io.aeron.archiver;

interface ArchiverProtocolListener
{
    void onClientInit(String channel, int streamId);
    void onArchiveStop(int correlationId, String channel, int streamId);

    void onArchiveStart(int correlationId, String channel1, int streamId);

    void onListStreamInstances(int correlationId, int from, int to);

    void onAbortReplay(int correlationId);

    void onStartReplay(
        int correlationId,
        int replayStreamId,
        String replayChannel,
        int persistedImageId,
        int termId,
        int termOffset,
        long length);
}

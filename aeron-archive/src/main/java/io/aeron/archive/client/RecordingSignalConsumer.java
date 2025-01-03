/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive.client;

import io.aeron.archive.codecs.RecordingSignal;

/**
 * Consumer of signals representing operations applied to a recording.
 */
@FunctionalInterface
public interface RecordingSignalConsumer
{
    /**
     * Signal of operation taken on a recording.
     *
     * @param controlSessionId that initiated the operation.
     * @param correlationId    that initiated the operation, could be the replication id.
     * @param recordingId      which has signalled.
     * @param subscriptionId   of the {@link io.aeron.Subscription} associated with the recording.
     * @param position         of the recorded stream at the point of signal.
     * @param signal           type of the operation applied to the recording.
     */
    void onSignal(
        long controlSessionId,
        long correlationId,
        long recordingId,
        long subscriptionId,
        long position,
        RecordingSignal signal);
}

/*
 * Copyright 2014-2021 Real Logic Limited.
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

import java.util.ArrayList;

class DelegatingRecordingSignalConsumer implements RecordingSignalConsumer
{
    private final ArrayList<RecordingSignalConsumer> consumers = new ArrayList<>();

    /**
     * {@inheritDoc}
     */
    public void onSignal(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long subscriptionId,
        final long position,
        final RecordingSignal signal)
    {
        for (int i = 0, n = consumers.size(); i < n; i++)
        {
            consumers.get(i).onSignal(controlSessionId, correlationId, recordingId, subscriptionId, position, signal);
        }
    }

    void addConsumer(final RecordingSignalConsumer consumer)
    {
        consumers.add(consumer);
    }

    void removeConsumer(final RecordingSignalConsumer consumer)
    {
        consumers.remove(consumer);
    }
}

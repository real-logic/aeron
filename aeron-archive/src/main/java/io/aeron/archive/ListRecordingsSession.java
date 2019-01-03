/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import org.agrona.concurrent.UnsafeBuffer;

class ListRecordingsSession extends AbstractListRecordingsSession
{
    private final long limitId;
    private long recordingId;

    ListRecordingsSession(
        final long correlationId,
        final long fromRecordingId,
        final int count,
        final Catalog catalog,
        final ControlResponseProxy proxy,
        final ControlSession controlSession,
        final UnsafeBuffer descriptorBuffer)
    {
        super(correlationId, catalog, proxy, controlSession, descriptorBuffer);

        recordingId = fromRecordingId;
        limitId = fromRecordingId + count;
    }

    protected int sendDescriptors()
    {
        int totalBytesSent = 0;
        int recordsScanned = 0;

        while (recordingId < limitId && recordsScanned < MAX_SCANS_PER_WORK_CYCLE)
        {
            if (!catalog.wrapDescriptor(recordingId, descriptorBuffer))
            {
                controlSession.sendRecordingUnknown(correlationId, recordingId, proxy);
                isDone = true;
                break;
            }

            if (Catalog.isValidDescriptor(descriptorBuffer))
            {
                final int bytesSent = controlSession.sendDescriptor(correlationId, descriptorBuffer, proxy);
                if (bytesSent == 0)
                {
                    isDone = controlSession.isDone();
                    break;
                }
                totalBytesSent += bytesSent;
            }

            ++recordingId;
            recordsScanned++;
        }

        if (recordingId >= limitId)
        {
            isDone = true;
        }

        return totalBytesSent;
    }
}

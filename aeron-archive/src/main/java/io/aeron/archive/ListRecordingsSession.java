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

import io.aeron.Publication;

import static io.aeron.archive.codecs.ControlResponseCode.RECORDING_NOT_FOUND;

class ListRecordingsSession extends AbstractListRecordingsSession
{
    private final long fromId;
    private final long toId;

    private long recordingId;

    ListRecordingsSession(
        final long correlationId,
        final Publication controlPublication,
        final long fromId,
        final int count,
        final Catalog catalog,
        final ControlSessionProxy proxy,
        final ControlSession controlSession)
    {
        super(correlationId, controlPublication, catalog, proxy, controlSession);
        this.recordingId = fromId;
        this.fromId = fromId;
        this.toId = fromId + count;
    }

    protected int sendDescriptors()
    {
        int sentBytes = 0;
        do
        {
            if (!catalog.wrapDescriptor(recordingId, descriptorBuffer))
            {
                proxy.sendDescriptorNotFound(
                    correlationId,
                    recordingId,
                    catalog.nextRecordingId(),
                    controlPublication);
                state = State.INACTIVE;

                return 0;
            }

            sentBytes += proxy.sendDescriptor(correlationId, descriptorBuffer, controlPublication);

            if (++recordingId >= toId)
            {
                state = State.INACTIVE;
                break;
            }
        }
        while (sentBytes < controlPublication.maxPayloadLength());

        return sentBytes;
    }

    protected int init()
    {
        if (fromId >= catalog.nextRecordingId())
        {
            sendError(RECORDING_NOT_FOUND, "Requested start id exceeds max allocated recording id");
            state = State.INACTIVE;
        }
        else
        {
            state = State.ACTIVE;
        }

        return 1;
    }
}

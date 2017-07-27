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
import io.aeron.archive.codecs.RecordingDescriptorDecoder;

import static io.aeron.archive.codecs.ControlResponseCode.RECORDING_NOT_FOUND;

class ListRecordingsForUriSession extends AbstractListRecordingsSession
{
    private static final int MAX_SCANS_PER_WORK_CYCLE = 16;

    private final RecordingDescriptorDecoder decoder;
    private final int count;
    private final String channel;
    private final int streamId;
    private long recordingId;
    private int sent = 0;

    ListRecordingsForUriSession(
        final long correlationId,
        final Publication controlPublication,
        final long fromRecordingId,
        final int count,
        final String channel,
        final int streamId,
        final Catalog catalog,
        final ControlSessionProxy proxy,
        final ControlSession controlSession,
        final RecordingDescriptorDecoder recordingDescriptorDecoder)
    {
        super(correlationId, controlPublication, catalog, proxy, controlSession);

        this.recordingId = fromRecordingId;
        this.count = count;
        this.channel = channel;
        this.streamId = streamId;
        this.decoder = recordingDescriptorDecoder;
    }

    protected int sendDescriptors()
    {
        int bytesSent = 0;
        int recordsScanned = 0;

        while (sent < count &&
               bytesSent < controlPublication.maxPayloadLength() &&
               recordsScanned < MAX_SCANS_PER_WORK_CYCLE)
        {
            if (!catalog.wrapDescriptor(recordingId, descriptorBuffer))
            {
                proxy.sendDescriptorNotFound(
                    correlationId,
                    recordingId,
                    catalog.nextRecordingId(),
                    controlPublication);
                state = State.INACTIVE;

                break;
            }

            recordingId++;
            recordsScanned++;

            decoder.wrap(
                descriptorBuffer,
                Catalog.CATALOG_FRAME_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);

            if (decoder.streamId() == streamId && decoder.strippedChannel().equals(channel))
            {
                bytesSent += proxy.sendDescriptor(correlationId, descriptorBuffer, controlPublication);

                if (++sent >= count)
                {
                    state = State.INACTIVE;
                }
            }
        }

        return bytesSent;
    }

    protected int init()
    {
        if (recordingId >= catalog.nextRecordingId())
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

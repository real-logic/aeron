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

class ListRecordingsForUriSession extends AbstractListRecordingsSession
{
    private static final int MAX_SCAN_PER_WORK = 16;
    private final RecordingDescriptorDecoder decoder;
    private final int fromIndex;
    private final int count;
    private final String channel;
    private final int streamId;
    private long recordingId;
    private int matched;
    private int sent;

    ListRecordingsForUriSession(
        final long correlationId,
        final Publication controlPublication,
        final int fromIndex,
        final int count,
        final String channel,
        final int streamId,
        final Catalog catalog,
        final ControlSessionProxy proxy,
        final ControlSession controlSession,
        final RecordingDescriptorDecoder recordingDescriptorDecoder)
    {
        super(correlationId, controlPublication, catalog, proxy, controlSession);

        this.fromIndex = fromIndex;
        this.count = count;
        this.channel = channel;
        this.streamId = streamId;
        this.decoder = recordingDescriptorDecoder;
    }

    protected int sendDescriptors()
    {
        int sentBytes = 0;
        int scanned = 0;
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

            recordingId++;
            scanned++;

            decoder.wrap(
                descriptorBuffer,
                Catalog.CATALOG_FRAME_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);

            if (decoder.streamId() != streamId || !decoder.strippedChannel().equals(channel))
            {
                continue;
            }

            if (matched++ < fromIndex)
            {
                continue;
            }

            sentBytes += proxy.sendDescriptor(correlationId, descriptorBuffer, controlPublication);

            if (sent++ >= count)
            {
                state = State.INACTIVE;
                break;
            }
        }
        while (sentBytes < controlPublication.maxPayloadLength() && scanned < MAX_SCAN_PER_WORK);

        return sentBytes;
    }

    protected int init()
    {
        state = State.ACTIVE;
        return 1;
    }
}

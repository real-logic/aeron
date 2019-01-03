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

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import org.agrona.concurrent.UnsafeBuffer;

class ListRecordingsForUriSession extends AbstractListRecordingsSession
{
    private long recordingId;
    private int sent = 0;
    private final int count;
    private final int streamId;
    private final byte[] channelFragment;
    private final RecordingDescriptorDecoder decoder;

    ListRecordingsForUriSession(
        final long correlationId,
        final long fromRecordingId,
        final int count,
        final byte[] channelFragment,
        final int streamId,
        final Catalog catalog,
        final ControlResponseProxy proxy,
        final ControlSession controlSession,
        final UnsafeBuffer descriptorBuffer,
        final RecordingDescriptorDecoder recordingDescriptorDecoder)
    {
        super(correlationId, catalog, proxy, controlSession, descriptorBuffer);

        this.recordingId = fromRecordingId;
        this.count = count;
        this.streamId = streamId;
        this.channelFragment = channelFragment;
        this.decoder = recordingDescriptorDecoder;
    }

    protected int sendDescriptors()
    {
        int totalBytesSent = 0;
        int recordsScanned = 0;

        while (sent < count && recordsScanned < MAX_SCANS_PER_WORK_CYCLE)
        {
            if (!catalog.wrapDescriptor(recordingId, descriptorBuffer))
            {
                controlSession.sendRecordingUnknown(correlationId, recordingId, proxy);

                isDone = true;
                break;
            }

            decoder.wrap(
                descriptorBuffer,
                RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);

            if (Catalog.isValidDescriptor(descriptorBuffer) &&
                decoder.streamId() == streamId &&
                Catalog.originalChannelContains(decoder, channelFragment))
            {
                final int bytesSent = controlSession.sendDescriptor(correlationId, descriptorBuffer, proxy);
                if (bytesSent == 0)
                {
                    isDone = controlSession.isDone();
                    break;
                }

                totalBytesSent += bytesSent;
                ++sent;
            }

            recordingId++;
            recordsScanned++;
        }

        if (sent >= count)
        {
            isDone = true;
        }

        return totalBytesSent;
    }
}

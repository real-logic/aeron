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

class ListRecordingsSession extends AbstractListRecordingsSession
{
    private final long limitId;

    ListRecordingsSession(
        final long correlationId,
        final Publication controlPublication,
        final long fromRecordingId,
        final int count,
        final Catalog catalog,
        final ControlSessionProxy proxy,
        final ControlSession controlSession)
    {
        super(correlationId, fromRecordingId, controlPublication, catalog, proxy, controlSession);

        this.limitId = fromRecordingId + count;
    }

    protected int sendDescriptors()
    {
        int sentBytes = 0;

        while (recordingId < limitId && sentBytes < controlPublication.maxPayloadLength())
        {
            if (!catalog.wrapDescriptor(recordingId, descriptorBuffer))
            {
                proxy.sendDescriptorNotFound(
                    correlationId,
                    recordingId,
                    catalog.nextRecordingId(),
                    controlPublication);
                state = State.INACTIVE;

                return sentBytes;
            }

            sentBytes += proxy.sendDescriptor(correlationId, descriptorBuffer, controlPublication);

            if (++recordingId >= limitId)
            {
                state = State.INACTIVE;
            }
        }

        return sentBytes;
    }
}

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

import io.aeron.archiver.codecs.*;
import io.aeron.logbuffer.*;
import org.agrona.DirectBuffer;

class ArchiverProtocolAdapter implements FragmentHandler
{
    private final ArchiverProtocolListener listener;
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final ReplayRequestDecoder replayRequestDecoder = new ReplayRequestDecoder();
    private final AbortReplayRequestDecoder abortReplayRequestDecoder = new AbortReplayRequestDecoder();
    private final ArchiveStartRequestDecoder archiveStartRequestDecoder = new ArchiveStartRequestDecoder();
    private final ArchiveStopRequestDecoder archiveStopRequestDecoder = new ArchiveStopRequestDecoder();
    private final ListStreamInstancesRequestDecoder listStreamInstancesRequestDecoder =
        new ListStreamInstancesRequestDecoder();
    private final ArchiverClientInitDecoder archiverClientInitDecoder = new ArchiverClientInitDecoder();

    ArchiverProtocolAdapter(final ArchiverProtocolListener listener)
    {
        this.listener = listener;
    }

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();
        // TODO: handle message versions

        switch (templateId)
        {
            case ArchiverClientInitDecoder.TEMPLATE_ID:
            {
                archiverClientInitDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onClientInit(
                    archiverClientInitDecoder.replyChannel(),
                    archiverClientInitDecoder.replyStreamId());
                break;
            }
            case ReplayRequestDecoder.TEMPLATE_ID:
            {
                replayRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onStartReplay(
                    replayRequestDecoder.correlationId(),
                    replayRequestDecoder.replayStreamId(),
                    replayRequestDecoder.replayChannel(),
                    replayRequestDecoder.persistedImageId(),
                    replayRequestDecoder.termId(),
                    replayRequestDecoder.termOffset(),
                    replayRequestDecoder.length());
                break;
            }
            case ArchiveStartRequestDecoder.TEMPLATE_ID:
            {
                archiveStartRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onArchiveStart(
                    archiveStartRequestDecoder.correlationId(),
                    archiveStartRequestDecoder.channel(),
                    archiveStartRequestDecoder.streamId());
                break;
            }
            case ArchiveStopRequestDecoder.TEMPLATE_ID:
            {
                archiveStopRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onArchiveStop(
                    archiveStopRequestDecoder.correlationId(),
                    archiveStopRequestDecoder.channel(),
                    archiveStopRequestDecoder.streamId());
                break;
            }
            case AbortReplayRequestDecoder.TEMPLATE_ID:
            {
                abortReplayRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onAbortReplay(
                    abortReplayRequestDecoder.correlationId());
                break;
            }
            case ListStreamInstancesRequestDecoder.TEMPLATE_ID:
            {
                listStreamInstancesRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onListStreamInstances(
                    listStreamInstancesRequestDecoder.correlationId(),
                    listStreamInstancesRequestDecoder.from(),
                    listStreamInstancesRequestDecoder.to());
                break;
            }
            default:
                throw new IllegalArgumentException("Unexpected template id:" + templateId);
        }
    }
}

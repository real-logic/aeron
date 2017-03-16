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

import io.aeron.archiver.messages.*;
import io.aeron.logbuffer.*;
import org.agrona.DirectBuffer;

class ArchiverProtocolAdapter implements FragmentHandler
{
    private final ArchiverProtocolListener listener;
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final ReplayRequestDecoder replayRequestDecoder = new ReplayRequestDecoder();
    private final ArchiveStartRequestDecoder archiveStartRequestDecoder = new ArchiveStartRequestDecoder();
    private final ArchiveStopRequestDecoder archiveStopRequestDecoder = new ArchiveStopRequestDecoder();
    private final ListStreamInstancesRequestDecoder requestDecoder =
        new ListStreamInstancesRequestDecoder();

    ArchiverProtocolAdapter(final ArchiverProtocolListener listener)
    {
        this.listener = listener;
    }

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();

        // TODO: handle message versions

        switch (templateId)
        {
            case ReplayRequestDecoder.TEMPLATE_ID:
                // validate image single use
                final int sessionId1 = header.sessionId();
                replayRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(),
                    headerDecoder.version());

                final int replyStreamId1 = replayRequestDecoder.replyStreamId();
                final String replyChannel = replayRequestDecoder.replyChannel();
                final int streamInstanceId = replayRequestDecoder.streamInstanceId();
                final int termId = replayRequestDecoder.termId();
                final int termOffset = replayRequestDecoder.termOffset();
                final long length1 = replayRequestDecoder.length();

                listener.onReplayStart(sessionId1,
                    replyStreamId1, replyChannel, streamInstanceId, termId, termOffset,
                    length1);

                break;

            case ArchiveStartRequestDecoder.TEMPLATE_ID:
                archiveStartRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final String channel1 = archiveStartRequestDecoder.channel();
                final int streamId = archiveStartRequestDecoder.streamId();

                listener.onArchiveStart(channel1, streamId);
                break;

            case ArchiveStopRequestDecoder.TEMPLATE_ID:
                archiveStopRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final String channel2 = archiveStopRequestDecoder.channel();
                final int streamId1 = archiveStopRequestDecoder.streamId();

                listener.onArchiveStop(channel2, streamId1);
                break;

            case AbortReplayRequestDecoder.TEMPLATE_ID:
                // TODO: replace with correlation id
                final int sessionId = header.sessionId();
                listener.onReplayStop(sessionId);
                break;

            case ListStreamInstancesRequestDecoder.TEMPLATE_ID:
                requestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final int from = requestDecoder.from();
                final int to = requestDecoder.to();
                final String channel = requestDecoder.replyChannel();
                final int replyStreamId = requestDecoder.replyStreamId();
                listener.onListStreamInstances(from, to, channel, replyStreamId);
                break;

            default:
                throw new IllegalArgumentException("Unexpected template id:" + templateId);
        }
    }
}

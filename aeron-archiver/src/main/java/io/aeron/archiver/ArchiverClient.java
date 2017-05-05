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

import io.aeron.*;
import io.aeron.archiver.codecs.*;
import org.agrona.*;

public class ArchiverClient
{
    public static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;
    private final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
    private final Publication archiverServiceRequest;
    private final Subscription archiverNotifications;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final ArchiverClientInitEncoder archiverClientInitEncoder;
    private final ArchiveStartRequestEncoder archiveStartRequestEncoder;
    private final ReplayRequestEncoder replayRequestEncoder;
    private final ArchiveStopRequestEncoder archiveStopRequestEncoder;
    private final ListStreamInstancesRequestEncoder listStreamInstancesRequestEncoder;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ArchiveProgressNotificationDecoder archiveProgressNotificationDecoder =
        new ArchiveProgressNotificationDecoder();
    private final ArchiveStartedNotificationDecoder archiveStartedNotificationDecoder =
        new ArchiveStartedNotificationDecoder();
    private final ArchiveStoppedNotificationDecoder archiveStoppedNotificationDecoder =
        new ArchiveStoppedNotificationDecoder();

    public ArchiverClient(
        final Publication archiverServiceRequest,
        final Subscription archiverNotifications)
    {
        this.archiverServiceRequest = archiverServiceRequest;
        this.archiverNotifications = archiverNotifications;
        messageHeaderEncoder = new MessageHeaderEncoder().wrap(buffer, 0);
        archiveStartRequestEncoder = new ArchiveStartRequestEncoder()
            .wrap(buffer, HEADER_LENGTH);
        replayRequestEncoder = new ReplayRequestEncoder()
            .wrap(buffer, HEADER_LENGTH);
        archiveStopRequestEncoder = new ArchiveStopRequestEncoder()
            .wrap(buffer, HEADER_LENGTH);
        listStreamInstancesRequestEncoder = new ListStreamInstancesRequestEncoder()
            .wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH);
        archiverClientInitEncoder = new ArchiverClientInitEncoder()
                .wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH);
    }

    public boolean clientInit(
        final String channel,
        final int streamId)
    {
        messageHeaderEncoder
            .templateId(ArchiverClientInitEncoder.TEMPLATE_ID)
            .blockLength(ArchiverClientInitEncoder.BLOCK_LENGTH)
            .schemaId(ArchiverClientInitEncoder.SCHEMA_ID)
            .version(ArchiverClientInitEncoder.SCHEMA_VERSION);

        archiverClientInitEncoder.limit(ArchiverClientInitEncoder.BLOCK_LENGTH + HEADER_LENGTH);

        archiverClientInitEncoder
            .replyChannel(channel)
            .replyStreamId(streamId);

        return offer(archiverClientInitEncoder.encodedLength());
    }

    public boolean requestArchiveStart(
        final String channel,
        final int streamId)
    {
        messageHeaderEncoder
            .templateId(ArchiveStartRequestEncoder.TEMPLATE_ID)
            .blockLength(ArchiveStartRequestEncoder.BLOCK_LENGTH)
            .schemaId(ArchiveStartRequestEncoder.SCHEMA_ID)
            .version(ArchiveStartRequestEncoder.SCHEMA_VERSION);

        archiveStartRequestEncoder.limit(ArchiveStartRequestEncoder.BLOCK_LENGTH + HEADER_LENGTH);

        archiveStartRequestEncoder
            .channel(channel)
            .streamId(streamId);

        return offer(archiveStartRequestEncoder.encodedLength());
    }

    public boolean requestArchiveStop(
        final String channel,
        final int streamId)
    {
        messageHeaderEncoder
            .templateId(ArchiveStopRequestEncoder.TEMPLATE_ID)
            .blockLength(ArchiveStopRequestEncoder.BLOCK_LENGTH)
            .schemaId(ArchiveStopRequestEncoder.SCHEMA_ID)
            .version(ArchiveStopRequestEncoder.SCHEMA_VERSION);

        archiveStartRequestEncoder.limit(ArchiveStopRequestEncoder.BLOCK_LENGTH + HEADER_LENGTH);

        archiveStopRequestEncoder
            .channel(channel)
            .streamId(streamId);

        return offer(archiveStopRequestEncoder.encodedLength());
    }

    public boolean requestReplay(
        final int streamInstanceId,
        final int termId,
        final int termOffset,
        final long length,
        final String replayChannel,
        final int replayStreamId)
    {
        messageHeaderEncoder
            .templateId(ReplayRequestEncoder.TEMPLATE_ID)
            .blockLength(ReplayRequestEncoder.BLOCK_LENGTH)
            .schemaId(ReplayRequestEncoder.SCHEMA_ID)
            .version(ReplayRequestEncoder.SCHEMA_VERSION);

        replayRequestEncoder.limit(ReplayRequestEncoder.BLOCK_LENGTH + HEADER_LENGTH);

        replayRequestEncoder
            .streamInstanceId(streamInstanceId)
            .termId(termId)
            .termOffset(termOffset)
            .length((int)length)
            .replayStreamId(replayStreamId)
            .replayChannel(replayChannel);

        return offer(replayRequestEncoder.encodedLength());
    }

    public boolean requestListStreamInstances(
        final int from,
        final int to)
    {
        messageHeaderEncoder
            .templateId(ListStreamInstancesRequestEncoder.TEMPLATE_ID)
            .blockLength(ListStreamInstancesRequestEncoder.BLOCK_LENGTH)
            .schemaId(ListStreamInstancesRequestEncoder.SCHEMA_ID)
            .version(ListStreamInstancesRequestEncoder.SCHEMA_VERSION);

        listStreamInstancesRequestEncoder.limit(ListStreamInstancesRequestEncoder.BLOCK_LENGTH + HEADER_LENGTH);

        listStreamInstancesRequestEncoder
            .from(from)
            .to(to);

        return offer(listStreamInstancesRequestEncoder.encodedLength());
    }

    interface ArchiverNotificationListener
    {
        void onProgress(
            int streamInstanceId,
            int initialTermId,
            int initialTermOffset,
            int termId,
            int termOffset);

        void onStart(
            int streamInstanceId,
            int sessionId,
            int streamId,
            String source,
            String channel);

        void onStop(int streamInstanceId);
    }

    public int pollNotifications(final ArchiverNotificationListener listener, final int count)
    {
        return archiverNotifications.poll((b, offset, length, header) ->
        {
            messageHeaderDecoder.wrap(b, offset);

            switch (messageHeaderDecoder.templateId())
            {
                case ArchiveProgressNotificationDecoder.TEMPLATE_ID:
                {
                    archiveProgressNotificationDecoder.wrap(
                            b,
                            offset + MessageHeaderDecoder.ENCODED_LENGTH,
                            messageHeaderDecoder.blockLength(),
                            messageHeaderDecoder.version());
                    listener.onProgress(
                        archiveProgressNotificationDecoder.streamInstanceId(),
                        archiveProgressNotificationDecoder.initialTermId(),
                        archiveProgressNotificationDecoder.initialTermOffset(),
                        archiveProgressNotificationDecoder.termId(),
                        archiveProgressNotificationDecoder.termOffset()
                    );
                    break;
                }

                case ArchiveStartedNotificationDecoder.TEMPLATE_ID:
                {
                    archiveStartedNotificationDecoder.wrap(
                        b,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.blockLength(),
                        messageHeaderDecoder.version());

                    listener.onStart(
                        archiveStartedNotificationDecoder.streamInstanceId(),
                        archiveStartedNotificationDecoder.sessionId(),
                        archiveStartedNotificationDecoder.streamId(),
                        archiveStartedNotificationDecoder.channel(),
                        archiveStartedNotificationDecoder.source());
                    break;
                }

                case ArchiveStoppedNotificationDecoder.TEMPLATE_ID:
                {
                    archiveStoppedNotificationDecoder.wrap(
                        b,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.blockLength(),
                        messageHeaderDecoder.version());

                    listener.onStop(archiveStoppedNotificationDecoder.streamInstanceId());
                    break;
                }

                default:
                    throw new IllegalStateException();
            }
        }, count);
    }

    private boolean offer(final int length)
    {
        final long newPosition = archiverServiceRequest.offer(buffer, 0, length + HEADER_LENGTH);

        return newPosition >= 0;
    }
}

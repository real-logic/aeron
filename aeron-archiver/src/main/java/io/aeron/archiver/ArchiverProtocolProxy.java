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
import io.aeron.archiver.messages.*;
import org.agrona.*;
import org.agrona.concurrent.*;
import uk.co.real_logic.sbe.ir.generated.MessageHeaderEncoder;

import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

class ArchiverProtocolProxy
{
    @FunctionalInterface
    interface PublishDirectBufferFunction
    {
        long offer(DirectBuffer buffer, int offset, int length);
    }

    // TODO: replace header usage with constants?
    private final IdleStrategy idleStrategy;
    private final Publication archiverNotifications;
    private final UnsafeBuffer outboundBuffer =
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(4096, CACHE_LINE_LENGTH));

    private final MessageHeaderEncoder outboundHeaderEncoder = new MessageHeaderEncoder();
    private final ArchiverResponseEncoder responseEncoder = new ArchiverResponseEncoder();
    private final ArchiveStartedNotificationEncoder archiveStartedNotificationEncoder =
        new ArchiveStartedNotificationEncoder();
    private final ArchiveProgressNotificationEncoder archiveProgressNotificationEncoder =
        new ArchiveProgressNotificationEncoder();
    private final ArchiveStoppedNotificationEncoder archiveStoppedNotificationEncoder =
        new ArchiveStoppedNotificationEncoder();

    ArchiverProtocolProxy(final IdleStrategy idleStrategy, final Publication archiverNotifications)
    {
        this.idleStrategy = idleStrategy;
        this.archiverNotifications = archiverNotifications;
        outboundHeaderEncoder.wrap(outboundBuffer, 0);
        responseEncoder.wrap(outboundBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
        archiveStartedNotificationEncoder.wrap(outboundBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
        archiveProgressNotificationEncoder.wrap(outboundBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
        archiveStoppedNotificationEncoder.wrap(outboundBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
    }

    void sendResponse(final ExclusivePublication responsePublication, final String err)
    {
        sendResponseF(responsePublication::offer, err);
    }

    void sendResponse(final Publication responsePublication, final String err)
    {
        sendResponseF(responsePublication::offer, err);
    }

    private void sendResponseF(final PublishDirectBufferFunction responsePublication, final String err)
    {
        outboundHeaderEncoder
            .blockLength(ArchiverResponseEncoder.BLOCK_LENGTH)
            .templateId(ArchiverResponseEncoder.TEMPLATE_ID)
            .schemaId(ArchiverResponseEncoder.SCHEMA_ID)
            .version(ArchiverResponseEncoder.SCHEMA_VERSION);

        // reset encoder limit is required for varible length messages
        responseEncoder.limit(MessageHeaderEncoder.ENCODED_LENGTH + ArchiverResponseEncoder.BLOCK_LENGTH);
        if (!Strings.isEmpty(err))
        {
            responseEncoder.err(err);
        }

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + responseEncoder.encodedLength();
        while (true)
        {
            final long result = responsePublication.offer(outboundBuffer, 0, length);
            if (result > 0)
            {
                idleStrategy.reset();
                break;
            }

            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED)
            {
                throw new IllegalStateException("Response channel is down: " + responsePublication);
            }

            idleStrategy.idle();
        }
    }

    int notifyArchiveStarted(
        final int instanceId,
        final String source,
        final int sessionId,
        final String channel,
        final int streamId)
    {
        outboundHeaderEncoder
            .blockLength(ArchiveStartedNotificationEncoder.BLOCK_LENGTH)
            .templateId(ArchiveStartedNotificationEncoder.TEMPLATE_ID)
            .schemaId(ArchiveStartedNotificationEncoder.SCHEMA_ID)
            .version(ArchiveStartedNotificationEncoder.SCHEMA_VERSION);

        // reset encoder limit is required for variable length messages
        responseEncoder.limit(MessageHeaderEncoder.ENCODED_LENGTH + ArchiveStartedNotificationEncoder.BLOCK_LENGTH);
        archiveStartedNotificationEncoder
            .streamInstanceId(instanceId)
            .sessionId(sessionId)
            .streamId(streamId)
            .source(source)
            .channel(channel);

        sendNotification(archiveStartedNotificationEncoder.encodedLength());

        return instanceId;
    }

    void notifyArchiveProgress(
        final int instanceId,
        final int initialTermId,
        final int initialTermOffset,
        final int termId,
        final int endTermOffset)
    {
        outboundHeaderEncoder
            .blockLength(ArchiveProgressNotificationEncoder.BLOCK_LENGTH)
            .templateId(ArchiveProgressNotificationEncoder.TEMPLATE_ID)
            .schemaId(ArchiveProgressNotificationEncoder.SCHEMA_ID)
            .version(ArchiveProgressNotificationEncoder.SCHEMA_VERSION);

        archiveProgressNotificationEncoder
            .streamInstanceId(instanceId)
            .initialTermId(initialTermId)
            .initialTermOffset(initialTermOffset)
            .termId(termId)
            .termOffset(endTermOffset);

        sendNotification(archiveProgressNotificationEncoder.encodedLength());
    }

    void notifyArchiveStopped(final int instanceId)
    {
        outboundHeaderEncoder
            .blockLength(ArchiveStoppedNotificationEncoder.BLOCK_LENGTH)
            .templateId(ArchiveStoppedNotificationEncoder.TEMPLATE_ID)
            .schemaId(ArchiveStoppedNotificationEncoder.SCHEMA_ID)
            .version(ArchiveStoppedNotificationEncoder.SCHEMA_VERSION);

        archiveStoppedNotificationEncoder.streamInstanceId(instanceId);
        sendNotification(archiveStoppedNotificationEncoder.encodedLength());
    }

    private void sendNotification(final int length)
    {
        while (true)
        {
            final long result = archiverNotifications.offer(
                outboundBuffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + length);
            if (result > 0 || result == Publication.NOT_CONNECTED)
            {
                idleStrategy.reset();
                break;
            }

            if (result == Publication.CLOSED)
            {
                throw new IllegalStateException();
            }

            idleStrategy.idle();
        }
    }
}

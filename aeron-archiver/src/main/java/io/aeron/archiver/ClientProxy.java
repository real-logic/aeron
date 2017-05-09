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
import org.agrona.concurrent.*;
import uk.co.real_logic.sbe.ir.generated.MessageHeaderEncoder;

import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

class ClientProxy
{
    private final IdleStrategy idleStrategy;
    private final Publication archiverNotifications;
    private final UnsafeBuffer outboundBuffer;

    private final MessageHeaderEncoder outboundHeaderEncoder = new MessageHeaderEncoder();
    private final ArchiverResponseEncoder responseEncoder = new ArchiverResponseEncoder();
    private final RecordingStartedEncoder recordingStartedEncoder = new RecordingStartedEncoder();
    private final RecordingProgressEncoder recordingProgressEncoder = new RecordingProgressEncoder();
    private final RecordingStoppedEncoder recordingStoppedEncoder = new RecordingStoppedEncoder();

    ClientProxy(final IdleStrategy idleStrategy, final Publication archiverNotifications)
    {
        this.idleStrategy = idleStrategy;
        this.archiverNotifications = archiverNotifications;

        //TODO: How will the buffer length be verified?
        final int maxPayloadAligned = BitUtil.align(archiverNotifications.maxPayloadLength(), 128);
        outboundBuffer =  new UnsafeBuffer(BufferUtil.allocateDirectAligned(maxPayloadAligned, CACHE_LINE_LENGTH));

        outboundHeaderEncoder.wrap(outboundBuffer, 0);
        responseEncoder.wrap(outboundBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
        recordingStartedEncoder.wrap(outboundBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
        recordingProgressEncoder.wrap(outboundBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
        recordingStoppedEncoder.wrap(outboundBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
    }

    void sendResponse(final ExclusivePublication reply, final String err, final int correlationId)
    {
        outboundHeaderEncoder
            .blockLength(ArchiverResponseEncoder.BLOCK_LENGTH)
            .templateId(ArchiverResponseEncoder.TEMPLATE_ID)
            .schemaId(ArchiverResponseEncoder.SCHEMA_ID)
            .version(ArchiverResponseEncoder.SCHEMA_VERSION);

        // reset encoder limit is required for variable length messages
        responseEncoder.limit(MessageHeaderEncoder.ENCODED_LENGTH + ArchiverResponseEncoder.BLOCK_LENGTH);
        responseEncoder.correlationId(correlationId);
        if (!Strings.isEmpty(err))
        {
            responseEncoder.err(err);
        }

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + responseEncoder.encodedLength();
        while (true)
        {
            final long result = reply.offer(outboundBuffer, 0, length);
            if (result > 0)
            {
                idleStrategy.reset();
                break;
            }

            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED)
            {
                throw new IllegalStateException("Response channel is down: " + reply);
            }

            idleStrategy.idle();
        }
    }


    int recordingStarted(
        final int recordingId,
        final String source,
        final int sessionId,
        final String channel,
        final int streamId)
    {
        outboundHeaderEncoder
            .blockLength(RecordingStartedEncoder.BLOCK_LENGTH)
            .templateId(RecordingStartedEncoder.TEMPLATE_ID)
            .schemaId(RecordingStartedEncoder.SCHEMA_ID)
            .version(RecordingStartedEncoder.SCHEMA_VERSION);

        // reset encoder limit is required for variable length messages
        recordingStartedEncoder
            .limit(MessageHeaderEncoder.ENCODED_LENGTH + RecordingStartedEncoder.BLOCK_LENGTH);

        recordingStartedEncoder
            .recordingId(recordingId)
            .sessionId(sessionId)
            .streamId(streamId)
            .source(source)
            .channel(channel);

        send(recordingStartedEncoder.encodedLength());

        return recordingId;
    }

    void recordingProgress(
        final int recordingId,
        final int initialTermId,
        final int initialTermOffset,
        final int termId,
        final int endTermOffset)
    {
        outboundHeaderEncoder
            .blockLength(RecordingProgressEncoder.BLOCK_LENGTH)
            .templateId(RecordingProgressEncoder.TEMPLATE_ID)
            .schemaId(RecordingProgressEncoder.SCHEMA_ID)
            .version(RecordingProgressEncoder.SCHEMA_VERSION);

        recordingProgressEncoder
            .recordingId(recordingId)
            .initialTermId(initialTermId)
            .initialTermOffset(initialTermOffset)
            .termId(termId)
            .termOffset(endTermOffset);

        send(recordingProgressEncoder.encodedLength());
    }

    void recordingStopped(final int recordingId)
    {
        outboundHeaderEncoder
            .blockLength(RecordingStoppedEncoder.BLOCK_LENGTH)
            .templateId(RecordingStoppedEncoder.TEMPLATE_ID)
            .schemaId(RecordingStoppedEncoder.SCHEMA_ID)
            .version(RecordingStoppedEncoder.SCHEMA_VERSION);

        recordingStoppedEncoder.recordingId(recordingId);
        send(recordingStoppedEncoder.encodedLength());
    }

    private void send(final int length)
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

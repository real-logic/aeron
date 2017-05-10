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

class ClientProxy
{
    private final IdleStrategy idleStrategy;
    private final Publication recordingNotifications;
    private final MutableDirectBuffer outboundBuffer = new ExpandableArrayBuffer();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ArchiverResponseEncoder responseEncoder = new ArchiverResponseEncoder();
    private final RecordingStartedEncoder recordingStartedEncoder = new RecordingStartedEncoder();
    private final RecordingProgressEncoder recordingProgressEncoder = new RecordingProgressEncoder();
    private final RecordingStoppedEncoder recordingStoppedEncoder = new RecordingStoppedEncoder();

    ClientProxy(final IdleStrategy idleStrategy, final Publication recordingNotifications)
    {
        this.idleStrategy = idleStrategy;
        this.recordingNotifications = recordingNotifications;
    }

    void sendResponse(final ExclusivePublication reply, final String err, final int correlationId)
    {
        responseEncoder
            .wrapAndApplyHeader(outboundBuffer, 0, messageHeaderEncoder)
            .correlationId(correlationId);

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
        recordingStartedEncoder
            .wrapAndApplyHeader(outboundBuffer, 0, messageHeaderEncoder)
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
        recordingProgressEncoder
            .wrapAndApplyHeader(outboundBuffer, 0, messageHeaderEncoder)
            .recordingId(recordingId)
            .initialTermId(initialTermId)
            .initialTermOffset(initialTermOffset)
            .termId(termId)
            .termOffset(endTermOffset);

        send(recordingProgressEncoder.encodedLength());
    }

    void recordingStopped(final int recordingId)
    {
        recordingStoppedEncoder
            .wrapAndApplyHeader(outboundBuffer, 0, messageHeaderEncoder)
            .recordingId(recordingId);

        send(recordingStoppedEncoder.encodedLength());
    }

    private void send(final int length)
    {
        while (true)
        {
            final long result = recordingNotifications.offer(
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

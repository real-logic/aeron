/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.*;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;

class RecordingEventsProxy
{
    private final IdleStrategy idleStrategy;
    private final Publication recordingEventsPublication;
    private final ExpandableArrayBuffer outboundBuffer = new ExpandableArrayBuffer(512);
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final RecordingStartedEncoder recordingStartedEncoder = new RecordingStartedEncoder();
    private final RecordingProgressEncoder recordingProgressEncoder = new RecordingProgressEncoder();
    private final RecordingStoppedEncoder recordingStoppedEncoder = new RecordingStoppedEncoder();

    RecordingEventsProxy(final IdleStrategy idleStrategy, final Publication recordingEventsPublication)
    {
        this.idleStrategy = idleStrategy;
        this.recordingEventsPublication = recordingEventsPublication;
    }

    void started(
        final long recordingId,
        final long startPosition,
        final int sessionId,
        final int streamId,
        final String channel,
        final String sourceIdentity)
    {
        recordingStartedEncoder
            .wrapAndApplyHeader(outboundBuffer, 0, messageHeaderEncoder)
            .recordingId(recordingId)
            .startPosition(startPosition)
            .sessionId(sessionId)
            .streamId(streamId)
            .channel(channel)
            .sourceIdentity(sourceIdentity);

        send(recordingStartedEncoder.encodedLength());
    }

    void progress(final long recordingId, final long startPosition, final long position)
    {
        recordingProgressEncoder
            .wrapAndApplyHeader(outboundBuffer, 0, messageHeaderEncoder)
            .recordingId(recordingId)
            .startPosition(startPosition)
            .position(position);

        send(recordingProgressEncoder.encodedLength());
    }

    void stopped(final long recordingId, final long startPosition, final long stopPosition)
    {
        recordingStoppedEncoder
            .wrapAndApplyHeader(outboundBuffer, 0, messageHeaderEncoder)
            .recordingId(recordingId)
            .startPosition(startPosition)
            .stopPosition(stopPosition);

        send(recordingStoppedEncoder.encodedLength());
    }

    private void send(final int length)
    {
        final int fullLength = MessageHeaderEncoder.ENCODED_LENGTH + length;
        while (true)
        {
            // TODO: Under back pressure it should drop sends and then do an update on timeout to avoid tail loss.
            final long result = recordingEventsPublication.offer(outboundBuffer, 0, fullLength);
            if (result > 0 || result == Publication.NOT_CONNECTED)
            {
                idleStrategy.reset();
                break;
            }

            if (result == Publication.CLOSED)
            {
                throw new ArchiveException("recording events publication is closed");
            }

            if (result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new ArchiveException("recording events publication at max position");
            }

            idleStrategy.idle();
        }
    }
}

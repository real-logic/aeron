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
package io.aeron.archiver;

import io.aeron.Publication;
import io.aeron.archiver.codecs.*;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;

class NotificationsProxy
{
    private final IdleStrategy idleStrategy;
    private final Publication recordingNotifications;
    private final ExpandableArrayBuffer outboundBuffer = new ExpandableArrayBuffer();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final RecordingStartedEncoder recordingStartedEncoder = new RecordingStartedEncoder();
    private final RecordingProgressEncoder recordingProgressEncoder = new RecordingProgressEncoder();
    private final RecordingStoppedEncoder recordingStoppedEncoder = new RecordingStoppedEncoder();

    NotificationsProxy(final IdleStrategy idleStrategy, final Publication recordingNotifications)
    {
        this.idleStrategy = idleStrategy;
        this.recordingNotifications = recordingNotifications;
    }

    void recordingStarted(
        final long recordingId,
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
    }

    void recordingProgress(
        final long recordingId,
        final long joiningPosition,
        final long currentPosition)
    {
        recordingProgressEncoder
            .wrapAndApplyHeader(outboundBuffer, 0, messageHeaderEncoder)
            .recordingId(recordingId)
            .joiningPosition(joiningPosition)
            .currentPosition(currentPosition);

        send(recordingProgressEncoder.encodedLength());
    }

    void recordingStopped(final long recordingId)
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

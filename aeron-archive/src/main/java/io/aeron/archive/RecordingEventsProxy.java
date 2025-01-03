/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import io.aeron.archive.codecs.MessageHeaderEncoder;
import io.aeron.archive.codecs.RecordingProgressEncoder;
import io.aeron.archive.codecs.RecordingStartedEncoder;
import io.aeron.archive.codecs.RecordingStoppedEncoder;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;

class RecordingEventsProxy implements AutoCloseable
{
    private static final int SEND_ATTEMPTS = 3;

    private final Publication publication;
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1024);
    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final RecordingStartedEncoder recordingStartedEncoder = new RecordingStartedEncoder();
    private final RecordingProgressEncoder recordingProgressEncoder = new RecordingProgressEncoder();
    private final RecordingStoppedEncoder recordingStoppedEncoder = new RecordingStoppedEncoder();

    RecordingEventsProxy(final Publication publication)
    {
        this.publication = publication;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(publication);
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
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .recordingId(recordingId)
            .startPosition(startPosition)
            .sessionId(sessionId)
            .streamId(streamId)
            .channel(channel)
            .sourceIdentity(sourceIdentity);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + recordingStartedEncoder.encodedLength();
        int attempts = SEND_ATTEMPTS;
        do
        {
            final long position = publication.offer(buffer, 0, length);
            if (position > 0 || Publication.NOT_CONNECTED == position)
            {
                break;
            }

            checkResult(position, publication);
        }
        while (--attempts > 0);
    }

    boolean progress(final long recordingId, final long startPosition, final long position)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + RecordingProgressEncoder.BLOCK_LENGTH;
        final long result = publication.tryClaim(length, bufferClaim);

        if (result > 0)
        {
            recordingProgressEncoder
                .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                .recordingId(recordingId)
                .startPosition(startPosition)
                .position(position);

            bufferClaim.commit();
            return true;
        }
        else
        {
            checkResult(result, publication);
        }

        return false;
    }

    void stopped(final long recordingId, final long startPosition, final long stopPosition)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + RecordingStoppedEncoder.BLOCK_LENGTH;
        int attempts = SEND_ATTEMPTS;

        do
        {
            final long position = publication.tryClaim(length, bufferClaim);
            if (position > 0)
            {
                recordingStoppedEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .recordingId(recordingId)
                    .startPosition(startPosition)
                    .stopPosition(stopPosition);

                bufferClaim.commit();
                break;
            }
            else if (Publication.NOT_CONNECTED == position)
            {
                break;
            }

            checkResult(position, publication);
        }
        while (--attempts > 0);
    }

    private static void checkResult(final long position, final Publication publication)
    {
        if (position == Publication.CLOSED)
        {
            throw new ArchiveException("recording events publication is closed");
        }

        if (position == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new ArchiveException(
                "recording events publication at max position, term-length=" + publication.termBufferLength());
        }
    }
}

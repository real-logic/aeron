/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import io.aeron.logbuffer.BufferClaim;
import org.agrona.CloseHelper;

class RecordingEventsProxy implements AutoCloseable
{
    private static final int SEND_ATTEMPTS = 3;

    private final Publication publication;
    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final RecordingStartedEncoder recordingStartedEncoder = new RecordingStartedEncoder();
    private final RecordingProgressEncoder recordingProgressEncoder = new RecordingProgressEncoder();
    private final RecordingStoppedEncoder recordingStoppedEncoder = new RecordingStoppedEncoder();

    RecordingEventsProxy(final Publication publication)
    {
        this.publication = publication;
    }

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
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + RecordingStartedEncoder.BLOCK_LENGTH +
            RecordingStartedEncoder.channelHeaderLength() + channel.length() +
            RecordingStartedEncoder.sourceIdentityHeaderLength() + sourceIdentity.length();

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                recordingStartedEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .recordingId(recordingId)
                    .startPosition(startPosition)
                    .sessionId(sessionId)
                    .streamId(streamId)
                    .channel(channel)
                    .sourceIdentity(sourceIdentity);

                bufferClaim.commit();
                break;
            }
            else if (Publication.NOT_CONNECTED == result)
            {
                break;
            }

            checkResult(result);
        }
        while (--attempts > 0);
    }

    void progress(final long recordingId, final long startPosition, final long position)
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
        }
        else
        {
            checkResult(result);
        }
    }

    void stopped(final long recordingId, final long startPosition, final long stopPosition)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + RecordingStoppedEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                recordingStoppedEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .recordingId(recordingId)
                    .startPosition(startPosition)
                    .stopPosition(stopPosition);

                bufferClaim.commit();
                break;
            }
            else if (Publication.NOT_CONNECTED == result)
            {
                break;
            }

            checkResult(result);
        }
        while (--attempts > 0);
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.CLOSED)
        {
            throw new ArchiveException("recording events publication is closed");
        }

        if (result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new ArchiveException("recording events publication at max position");
        }
    }
}

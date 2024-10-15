/*
 * Copyright 2014-2024 Real Logic Limited.
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

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;

class CreateReplayPublicationSession implements Session
{
    private final long correlationId;
    private final long recordingId;
    private final long replayPosition;
    private final long replayLength;
    private final long startPosition;
    private final long stopPosition;
    private final int segmentFileLength;
    private final int termBufferLength;
    private final int streamId;
    private long publicationRegistrationId;
    private final int fileIoMaxLength;
    private boolean isDone = false;
    private final Aeron aeron;
    private final Counter limitPositionCounter;
    private final ControlSession controlSession;
    private final ArchiveConductor conductor;

    CreateReplayPublicationSession(
        final long correlationId,
        final long recordingId,
        final long replayPosition,
        final long replayLength,
        final long startPosition,
        final long stopPosition,
        final int segmentFileLength,
        final int termBufferLength,
        final int streamId,
        final long publicationRegistrationId,
        final int fileIoMaxLength,
        final Counter limitPositionCounter,
        final Aeron aeron,
        final ControlSession controlSession,
        final ArchiveConductor conductor)
    {
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.replayPosition = replayPosition;
        this.replayLength = replayLength;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.segmentFileLength = segmentFileLength;
        this.termBufferLength = termBufferLength;
        this.streamId = streamId;
        this.publicationRegistrationId = publicationRegistrationId;
        this.fileIoMaxLength = fileIoMaxLength;
        this.limitPositionCounter = limitPositionCounter;
        this.aeron = aeron;
        this.controlSession = controlSession;
        this.conductor = conductor;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        if (Aeron.NULL_VALUE != publicationRegistrationId)
        {
            aeron.asyncRemovePublication(publicationRegistrationId);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void abort()
    {
        isDone = true;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isDone()
    {
        return isDone;
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return publicationRegistrationId;
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = 0;

        if (!isDone)
        {
            final ExclusivePublication publication;
            try
            {
                publication = aeron.getExclusivePublication(publicationRegistrationId);
            }
            catch (final Exception ex)
            {
                isDone = true;
                final String msg = "failed to create replay publication: " + ex.getMessage();
                controlSession.sendErrorResponse(correlationId, msg);
                throw ex;
            }

            if (null != publication)
            {
                publicationRegistrationId = Aeron.NULL_VALUE;
                isDone = true;
                workCount += 1;

                conductor.newReplaySession(
                    correlationId,
                    recordingId,
                    replayPosition,
                    replayLength,
                    startPosition,
                    stopPosition,
                    segmentFileLength,
                    termBufferLength,
                    streamId,
                    fileIoMaxLength,
                    controlSession,
                    limitPositionCounter,
                    publication);
            }
        }

        return workCount;
    }
}

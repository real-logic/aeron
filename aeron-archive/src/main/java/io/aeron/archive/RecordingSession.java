/*
 * Copyright 2014-2020 Real Logic Limited.
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

import io.aeron.*;
import io.aeron.archive.checksum.Checksum;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.channels.FileChannel;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

/**
 * Consumes an {@link Image} and records data to file using a {@link RecordingWriter}.
 */
class RecordingSession implements Session
{
    private enum State
    {
        INIT, RECORDING, INACTIVE, STOPPED
    }

    private final long correlationId;
    private final long recordingId;
    private long progressEventPosition;
    private final int blockLengthLimit;
    private final boolean autoStop;
    private final RecordingEventsProxy recordingEventsProxy;
    private final Image image;
    private final Counter position;
    private final RecordingWriter recordingWriter;
    private State state = State.INIT;
    private final String originalChannel;
    private final ControlSession controlSession;
    private final CountedErrorHandler countedErrorHandler;

    RecordingSession(
        final long correlationId,
        final long recordingId,
        final long startPosition,
        final int segmentLength,
        final String originalChannel,
        final RecordingEventsProxy recordingEventsProxy,
        final Image image,
        final Counter position,
        final FileChannel archiveDirChannel,
        final Archive.Context ctx,
        final ControlSession controlSession,
        final UnsafeBuffer checksumBuffer,
        final Checksum checksum,
        final boolean autoStop)
    {
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.originalChannel = originalChannel;
        this.recordingEventsProxy = recordingEventsProxy;
        this.image = image;
        this.position = position;
        this.controlSession = controlSession;
        this.autoStop = autoStop;
        countedErrorHandler = ctx.countedErrorHandler();
        progressEventPosition = image.joinPosition();

        blockLengthLimit = Math.min(image.termBufferLength(), ctx.fileIoMaxLength());
        recordingWriter = new RecordingWriter(
            recordingId, startPosition, segmentLength, image, ctx, archiveDirChannel, checksumBuffer, checksum);
    }

    public long correlationId()
    {
        return correlationId;
    }

    public long sessionId()
    {
        return recordingId;
    }

    public boolean isDone()
    {
        return state == State.STOPPED;
    }

    public void abort()
    {
        state(State.INACTIVE);
    }

    public void close()
    {
        if (autoStop)
        {
            final Subscription subscription = image.subscription();
            CloseHelper.close(countedErrorHandler, subscription);
            controlSession.archiveConductor().removeRecordingSubscription(subscription.registrationId());
        }
        CloseHelper.close(countedErrorHandler, position);
        recordingWriter.close();
    }

    public void abortClose()
    {
        recordingWriter.close();
    }

    public Counter recordingPosition()
    {
        return position;
    }

    public long recordedPosition()
    {
        if (position.isClosed())
        {
            return NULL_POSITION;
        }

        return position.get();
    }

    public int doWork()
    {
        int workCount = 0;

        if (State.INIT == state)
        {
            workCount += init();
        }

        if (State.RECORDING == state)
        {
            workCount += record();
        }

        if (State.INACTIVE == state)
        {
            state(State.STOPPED);

            if (null != recordingEventsProxy)
            {
                recordingEventsProxy.stopped(recordingId, image.joinPosition(), position.getWeak());
            }

            recordingWriter.close();
            workCount += 1;
        }

        return workCount;
    }

    Subscription subscription()
    {
        return image.subscription();
    }

    ControlSession controlSession()
    {
        return controlSession;
    }

    private int init()
    {
        try
        {
            recordingWriter.init();
        }
        catch (final IOException ex)
        {
            recordingWriter.close();
            state(State.STOPPED);
            LangUtil.rethrowUnchecked(ex);
        }

        if (null != recordingEventsProxy)
        {
            recordingEventsProxy.started(
                recordingId,
                image.joinPosition(),
                image.sessionId(),
                image.subscription().streamId(),
                originalChannel,
                image.sourceIdentity());
        }

        state(State.RECORDING);

        return 1;
    }

    private int record()
    {
        int workCount = 0;
        try
        {
            workCount = image.blockPoll(recordingWriter, blockLengthLimit);

            if (recordingWriter.isClosed())
            {
                state(State.INACTIVE);
                return workCount;
            }

            if (workCount > 0)
            {
                this.position.setOrdered(recordingWriter.position());
            }
            else if (image.isEndOfStream() || image.isClosed())
            {
                state(State.INACTIVE);
            }

            if (null != recordingEventsProxy)
            {
                final long recordedPosition = recordingWriter.position();
                if (progressEventPosition < recordedPosition)
                {
                    if (recordingEventsProxy.progress(recordingId, image.joinPosition(), recordedPosition))
                    {
                        progressEventPosition = recordedPosition;
                    }
                }
            }
        }
        catch (final Exception ex)
        {
            state(State.INACTIVE);
            LangUtil.rethrowUnchecked(ex);
        }

        return workCount;
    }

    private void state(final State newState)
    {
        //System.out.println("RecordingSession: " + state + " -> " + newState);
        state = newState;
    }
}

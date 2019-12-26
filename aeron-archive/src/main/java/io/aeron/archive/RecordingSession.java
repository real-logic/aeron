/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.Counter;
import io.aeron.Image;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
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
    private final int blockLengthLimit;
    private final RecordingEventsProxy recordingEventsProxy;
    private final Image image;
    private final Counter position;
    private final RecordingWriter recordingWriter;
    private State state = State.INIT;
    private final String originalChannel;
    private final ControlSession controlSession;

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
        final UnsafeBuffer recordingBuffer)
    {
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.originalChannel = originalChannel;
        this.recordingEventsProxy = recordingEventsProxy;
        this.image = image;
        this.position = position;
        this.controlSession = controlSession;

        blockLengthLimit = Math.min(image.termBufferLength(), Archive.Configuration.MAX_BLOCK_LENGTH);
        recordingWriter = new RecordingWriter(
            recordingId, startPosition, segmentLength, image, ctx, archiveDirChannel, recordingBuffer);
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
        this.state = State.INACTIVE;
    }

    public void close()
    {
        recordingWriter.close();
        CloseHelper.close(position);
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
            state = State.STOPPED;
            recordingEventsProxy.stopped(recordingId, image.joinPosition(), image.position());
            recordingWriter.close();
            workCount += 1;
        }

        return workCount;
    }

    Image image()
    {
        return image;
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
            state = State.STOPPED;
            LangUtil.rethrowUnchecked(ex);
        }

        recordingEventsProxy.started(
            recordingId,
            image.joinPosition(),
            image.sessionId(),
            image.subscription().streamId(),
            originalChannel,
            image.sourceIdentity());

        state = State.RECORDING;

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
                state = State.INACTIVE;
            }
            else if (workCount > 0)
            {
                final long position = image.position();
                this.position.setOrdered(position);
                recordingEventsProxy.progress(recordingId, image.joinPosition(), position);
            }
            else if (image.isEndOfStream() || image.isClosed())
            {
                state = State.INACTIVE;
            }
        }
        catch (final Exception ex)
        {
            state = State.INACTIVE;
            LangUtil.rethrowUnchecked(ex);
        }

        return workCount;
    }
}

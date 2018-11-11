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

import io.aeron.Counter;
import io.aeron.Image;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Consumes an {@link Image} and records data to file using a {@link RecordingWriter}.
 */
class RecordingSession implements Session
{
    private static final int MAX_BLOCK_LENGTH = 2 * 1204 * 1024;

    private enum State
    {
        INIT, RECORDING, INACTIVE, STOPPED
    }

    private final long recordingId;
    private final int blockLengthLimit;
    private final RecordingEventsProxy recordingEventsProxy;
    private final Image image;
    private final Counter position;
    private final RecordingWriter recordingWriter;
    private State state = State.INIT;
    private final String originalChannel;

    RecordingSession(
        final long recordingId,
        final long startPosition,
        final int segmentLength,
        final String originalChannel,
        final RecordingEventsProxy recordingEventsProxy,
        final Image image,
        final Counter position,
        final FileChannel archiveDirChannel,
        final Archive.Context ctx)
    {
        this.recordingId = recordingId;
        this.originalChannel = originalChannel;
        this.recordingEventsProxy = recordingEventsProxy;
        this.image = image;
        this.position = position;

        blockLengthLimit = Math.min(image.termBufferLength(), MAX_BLOCK_LENGTH);
        recordingWriter = new RecordingWriter(recordingId, startPosition, segmentLength, image, ctx, archiveDirChannel);
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

    public Counter recordingPosition()
    {
        return position;
    }

    public int doWork()
    {
        int workDone = 0;

        if (State.INIT == state)
        {
            workDone += init();
        }

        if (State.RECORDING == state)
        {
            workDone += record();
        }

        if (State.INACTIVE == state)
        {
            state = State.STOPPED;
            recordingEventsProxy.stopped(recordingId, image.joinPosition(), image.position());
            recordingWriter.close();
            workDone += 1;
        }

        return workDone;
    }

    private int init()
    {
        try
        {
            recordingWriter.init();
        }
        catch (final IOException ex)
        {
            close();
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
            if (workCount > 0)
            {
                final long position = image.position();
                this.position.setOrdered(position);
                recordingEventsProxy.progress(recordingId, image.joinPosition(), position);
            }
            else if (image.isEndOfStream() || image.isClosed())
            {
                this.state = State.INACTIVE;
            }

            if (recordingWriter.isClosed())
            {
                this.state = State.INACTIVE;
            }
        }
        catch (final Exception ex)
        {
            this.state = State.INACTIVE;
            LangUtil.rethrowUnchecked(ex);
        }

        return workCount;
    }
}

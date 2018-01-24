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
package io.aeron.archive;

import io.aeron.Counter;
import io.aeron.Image;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;

import java.nio.channels.FileChannel;

/**
 * Consumes an {@link Image} and records data to file using an {@link RecordingWriter}.
 */
class RecordingSession implements Session
{
    private static final int MAX_BLOCK_LENGTH = 16 * 1204 * 1024;

    private enum State
    {
        INIT, RECORDING, INACTIVE, STOPPED, CLOSED
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
        final String originalChannel,
        final RecordingEventsProxy recordingEventsProxy,
        final Image image,
        final Counter position,
        final FileChannel archiveDirChannel,
        final Archive.Context context)
    {
        this.recordingId = recordingId;
        this.originalChannel = originalChannel;
        this.recordingEventsProxy = recordingEventsProxy;
        this.image = image;
        this.position = position;

        final int termBufferLength = image.termBufferLength();
        blockLengthLimit = Math.min(termBufferLength, MAX_BLOCK_LENGTH);

        recordingWriter = new RecordingWriter(
            recordingId, startPosition, image.joinPosition(), termBufferLength, context, archiveDirChannel, position);
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
        if (State.CLOSED != state)
        {
            state = State.CLOSED;
            CloseHelper.close(position);
            recordingWriter.close();
        }
    }

    public Counter recordingPosition()
    {
        return position;
    }

    public int doWork()
    {
        int workDone = 0;

        switch (state)
        {
            case INIT:
                workDone = init();
                break;

            case RECORDING:
                workDone = record();
                break;

            case INACTIVE:
                recordingEventsProxy.stopped(recordingId, image.joinPosition(), position.getWeak());
                state = State.STOPPED;
                workDone = 1;
                break;
        }

        return workDone;
    }

    private int init()
    {
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
        int workCount = 1;
        try
        {
            workCount = image.rawPoll(recordingWriter, blockLengthLimit);
            if (0 != workCount)
            {
                recordingEventsProxy.progress(recordingId, image.joinPosition(), position.getWeak());
            }

            if (image.isClosed() || recordingWriter.isClosed())
            {
                abort();
            }
        }
        catch (final Exception ex)
        {
            abort();
            LangUtil.rethrowUnchecked(ex);
        }

        return workCount;
    }
}

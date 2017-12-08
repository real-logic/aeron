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
        INIT, RECORDING, INACTIVE, CLOSED
    }

    private final long recordingId;
    private final int blockLengthLimit;
    private final RecordingEventsProxy recordingEventsProxy;
    private final Image image;
    private final Counter position;
    private final RecordingWriter recordingWriter;
    private State state = State.INIT;

    RecordingSession(
        final long recordingId,
        final RecordingEventsProxy recordingEventsProxy,
        final Image image,
        final Counter position,
        final FileChannel archiveDirChannel,
        final Archive.Context context)
    {
        this.recordingId = recordingId;
        this.recordingEventsProxy = recordingEventsProxy;
        this.image = image;
        this.position = position;

        final int termBufferLength = image.termBufferLength();
        blockLengthLimit = Math.min(termBufferLength, MAX_BLOCK_LENGTH);
        final long startPosition = image.joinPosition();

        position.setOrdered(startPosition);

        RecordingWriter recordingWriter = null;
        try
        {
            recordingWriter = new RecordingWriter(recordingId, termBufferLength, context, archiveDirChannel, position);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }

        this.recordingWriter = recordingWriter;
    }

    public long recordingId()
    {
        return recordingId;
    }

    public boolean isDone()
    {
        return state == State.INACTIVE;
    }

    public void abort()
    {
        this.state = State.INACTIVE;
        CloseHelper.quietClose(recordingWriter);
    }

    public int doWork()
    {
        int workDone = 0;

        switch (state)
        {
            case INIT:
                workDone += init();
                break;

            case RECORDING:
                workDone += record();
                break;

            case INACTIVE:
                recordingWriter.close();
                break;
        }

        return workDone;
    }

    public long sessionId()
    {
        return recordingId;
    }

    private int init()
    {
        recordingEventsProxy.started(
            recordingId,
            image.joinPosition(),
            image.sessionId(),
            image.subscription().streamId(),
            image.subscription().channel(),
            image.sourceIdentity());

        state = State.RECORDING;

        return 1;
    }

    public void close()
    {
        state = State.CLOSED;
        recordingEventsProxy.stopped(recordingId, image.joinPosition(), position.getWeak());
        CloseHelper.quietClose(recordingWriter);
    }

    private int record()
    {
        int workCount = 1;
        try
        {
            workCount = image.rawPoll(recordingWriter, blockLengthLimit);
            if (workCount != 0)
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

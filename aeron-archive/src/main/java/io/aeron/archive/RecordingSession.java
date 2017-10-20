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

import io.aeron.Image;
import io.aeron.Subscription;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.status.AtomicCounter;

import java.nio.channels.FileChannel;

import static io.aeron.archive.Catalog.NULL_POSITION;

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
    private final String strippedChannel;
    private final Image image;
    private final AtomicCounter position;
    private final FileChannel archiveDirChannel;
    private final Archive.Context context;

    private RecordingWriter recordingWriter;
    private State state = State.INIT;

    RecordingSession(
        final long recordingId,
        final RecordingEventsProxy recordingEventsProxy,
        final String strippedChannel,
        final Image image,
        final AtomicCounter position,
        final FileChannel archiveDirChannel,
        final Archive.Context context)
    {
        this.recordingId = recordingId;
        this.recordingEventsProxy = recordingEventsProxy;
        this.strippedChannel = strippedChannel;
        this.image = image;
        this.position = position;
        this.archiveDirChannel = archiveDirChannel;
        this.context = context;

        blockLengthLimit = Math.min(image.termBufferLength(), MAX_BLOCK_LENGTH);
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
        final Subscription subscription = image.subscription();
        final int sessionId = image.sessionId();
        final int streamId = subscription.streamId();
        final int termBufferLength = image.termBufferLength();
        final String sourceIdentity = image.sourceIdentity();
        final long startPosition = image.joinPosition();

        RecordingWriter recordingWriter = null;
        try
        {
            recordingWriter = new RecordingWriter(
                recordingId, startPosition, termBufferLength, context, archiveDirChannel, position);
        }
        catch (final Exception ex)
        {
            state = State.INACTIVE;
            close();
            LangUtil.rethrowUnchecked(ex);
        }

        recordingEventsProxy.started(
            recordingId,
            startPosition,
            sessionId,
            streamId,
            strippedChannel,
            sourceIdentity);

        this.recordingWriter = recordingWriter;
        state = State.RECORDING;

        return 1;
    }

    public void close()
    {
        state = State.CLOSED;
        if (recordingWriter != null)
        {
            final long startPosition = recordingWriter.startPosition();
            final long stopPosition = recordingWriter.recordedPosition();
            CloseHelper.quietClose(recordingWriter);
            recordingEventsProxy.stopped(recordingId, startPosition, stopPosition);
        }
        else
        {
            recordingEventsProxy.stopped(recordingId, NULL_POSITION, NULL_POSITION);
        }
    }

    private int record()
    {
        int workCount = 1;
        try
        {
            workCount = image.rawPoll(recordingWriter, blockLengthLimit);
            if (workCount != 0)
            {
                recordingEventsProxy.progress(
                    recordingWriter.recordingId(),
                    recordingWriter.startPosition(),
                    recordingWriter.recordedPosition());
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

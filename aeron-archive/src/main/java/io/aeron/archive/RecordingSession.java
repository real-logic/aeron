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
import io.aeron.archive.RecordingWriter.Context;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Consumes an {@link Image} and records data to file using an {@link RecordingWriter}.
 */
class RecordingSession implements Session
{
    private enum State
    {
        INIT, RECORDING, INACTIVE, CLOSED
    }

    private final long recordingId;
    private final UnsafeBuffer descriptorBuffer;
    private final RecordingEventsProxy recordingEventsProxy;
    private final Image image;
    private final AtomicCounter position;
    private final RecordingWriter.Context context;

    private RecordingWriter recordingWriter;
    private State state = State.INIT;

    RecordingSession(
        final long recordingId,
        final UnsafeBuffer descriptorBuffer,
        final RecordingEventsProxy recordingEventsProxy,
        final Image image,
        final AtomicCounter position,
        final Context context)
    {
        this.recordingId = recordingId;
        this.descriptorBuffer = descriptorBuffer;
        this.recordingEventsProxy = recordingEventsProxy;
        this.image = image;
        this.position = position;
        this.context = context;
    }

    public boolean isDone()
    {
        return state == State.INACTIVE;
    }

    public void abort()
    {
        this.state = State.INACTIVE;
    }

    public int doWork()
    {
        int workDone = 0;

        if (state == State.INIT)
        {
            workDone += init();
        }

        if (state == State.RECORDING)
        {
            workDone += record();
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
        final String channel = subscription.channel();
        final String sourceIdentity = image.sourceIdentity();
        final long startPosition = image.joinPosition();

        RecordingWriter recordingWriter = null;
        try
        {
            recordingWriter = new RecordingWriter(context, descriptorBuffer, position);
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
            channel,
            sourceIdentity);

        this.recordingWriter = recordingWriter;
        this.state = State.RECORDING;

        return 1;
    }

    public void close()
    {
        CloseHelper.quietClose(recordingWriter);
        recordingEventsProxy.stopped(recordingId, recordingWriter.startPosition(), recordingWriter.stopPosition());
        state = State.CLOSED;
    }

    private int record()
    {
        int workCount = 1;
        try
        {
            workCount = image.rawPoll(recordingWriter, recordingWriter.segmentFileLength());
            if (workCount != 0)
            {
                recordingEventsProxy.progress(
                    recordingWriter.recordingId(),
                    recordingWriter.startPosition(),
                    recordingWriter.stopPosition());
            }

            if (image.isClosed() || recordingWriter.isClosed())
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

    long stopPosition()
    {
        return recordingWriter != null ? recordingWriter.stopPosition() : Catalog.NULL_POSITION;
    }
}

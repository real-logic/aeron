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
package io.aeron.archiver;

import io.aeron.*;
import org.agrona.*;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Consumes an {@link Image} and records data to file using an {@link RecordingWriter}.
 */
class RecordingSession implements Session
{
    private enum State
    {
        INIT, RECORDING, INACTIVE, CLOSED
    }

    private long recordingId = Catalog.NULL_RECORD_ID;
    private final NotificationsProxy notificationsProxy;
    private final Image image;
    private final Catalog catalog;
    private final RecordingWriter.RecordingContext recordingContext;

    private RecordingWriter recordingWriter;
    private State state = State.INIT;

    RecordingSession(
        final NotificationsProxy notificationsProxy,
        final Catalog catalog,
        final Image image,
        final RecordingWriter.RecordingContext recordingContext)
    {
        this.notificationsProxy = notificationsProxy;
        this.image = image;
        this.catalog = catalog;
        this.recordingContext = recordingContext;
    }

    public boolean isDone()
    {
        return state == State.CLOSED;
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

        if (state == State.INACTIVE)
        {
            workDone += close();
        }

        return workDone;
    }

    ByteBuffer metaDataBuffer()
    {
        return recordingWriter.metaDataBuffer();
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
        final int termBufferLength = image.termBufferLength();
        final int mtuLength = image.mtuLength();
        final int initialTermId = image.initialTermId();
        final long joiningPosition = image.joiningPosition();

        RecordingWriter recordingWriter = null;
        try
        {
            recordingId = catalog.addNewRecording(
                sessionId,
                streamId,
                channel,
                sourceIdentity,
                termBufferLength,
                mtuLength,
                initialTermId,
                joiningPosition,
                this,
                recordingContext.recordingFileLength());

            notificationsProxy.recordingStarted(
                recordingId,
                sessionId,
                streamId,
                channel,
                sourceIdentity
            );

            recordingWriter = new RecordingWriter(
                recordingContext,
                recordingId,
                termBufferLength,
                mtuLength,
                initialTermId,
                joiningPosition,
                sessionId,
                streamId,
                channel,
                sourceIdentity);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }

        this.recordingWriter = recordingWriter;
        this.state = State.RECORDING;

        return 1;
    }

    private int close()
    {
        try
        {
            if (recordingWriter != null)
            {
                recordingWriter.stop();
                catalog.updateCatalogFromMeta(recordingId, recordingWriter.metaDataBuffer());
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            CloseHelper.quietClose(recordingWriter);
            // this reflects the single local recording assumption
            // TODO: if we want a NoOp client lock in the DEDICATED mode this needs to be done on the conductor
            CloseHelper.quietClose(image.subscription());
            notificationsProxy.recordingStopped(recordingId, recordingWriter.lastPosition());
            this.state = State.CLOSED;
        }

        return 1;
    }

    private int record()
    {
        int workCount = 1;
        try
        {
            workCount = image.rawPoll(recordingWriter, recordingWriter.segmentFileLength());
            if (workCount != 0)
            {
                notificationsProxy.recordingProgress(
                    recordingWriter.recordingId(),
                    recordingWriter.joiningPosition(),
                    recordingWriter.lastPosition());
            }

            if (image.isClosed())
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

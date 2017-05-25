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
 * Consumes an {@link Image} and records data to file using an {@link Recorder}.
 */
class RecordingSession implements ArchiveConductor.Session
{
    private enum State
    {
        INIT, RECORDING, INACTIVE, CLOSED
    }

    private long recordingId = Catalog.NULL_RECORD_ID;
    private final NotificationsProxy notificationsProxy;
    private final Image image;
    private final Catalog catalog;
    private final Recorder.Builder builder;

    private Recorder recorder;
    private State state = State.INIT;

    RecordingSession(
        final NotificationsProxy notificationsProxy,
        final Catalog catalog,
        final Image image,
        final Recorder.Builder builder)
    {
        this.notificationsProxy = notificationsProxy;
        this.image = image;
        this.catalog = catalog;
        this.builder = builder;
    }

    public boolean isDone()
    {
        return state == State.CLOSED;
    }

    public void remove(final ArchiveConductor conductor)
    {
        catalog.removeRecordingSession(recordingId);
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
        return recorder.metaDataBuffer();
    }

    long recordingId()
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

        Recorder recorder = null;
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
                builder.recordingFileLength());

            notificationsProxy.recordingStarted(
                recordingId,
                sessionId,
                streamId,
                channel,
                sourceIdentity
            );

            recorder = builder
                .recordingId(recordingId)
                .termBufferLength(termBufferLength)
                .sessionId(sessionId)
                .streamId(streamId)
                .channel(channel)
                .sourceIdentity(sourceIdentity)
                .initialTermId(initialTermId)
                .mtuLength(mtuLength)
                .joiningPosition(joiningPosition)
                .build();
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }

        this.recorder = recorder;
        this.state = State.RECORDING;

        return 1;
    }

    private int close()
    {
        try
        {
            if (recorder != null)
            {
                recorder.stop();
                catalog.updateCatalogFromMeta(recordingId, recorder.metaDataBuffer());
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            CloseHelper.quietClose(recorder);
            // this reflects the single local recording assumption
            CloseHelper.quietClose(image.subscription());
            notificationsProxy.recordingStopped(recordingId);
            this.state = State.CLOSED;
        }

        return 1;
    }

    private int record()
    {
        int workCount = 1;
        try
        {
            workCount = image.rawPoll(recorder, recorder.segmentFileLength());
            if (workCount != 0)
            {
                notificationsProxy.recordingProgress(
                    recorder.recordingId(),
                    recorder.joiningPosition(),
                    recorder.lastPosition());
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

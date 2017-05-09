/*
 * Copyright 2014-2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archiver;

import io.aeron.*;
import org.agrona.*;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Consumes an {@link Image} and records data to file using an {@link ImageRecorder}.
 */
class RecordingSession implements ArchiveConductor.Session
{
    private enum State
    {
        INIT, ARCHIVING, CLOSING, DONE
    }

    private int recordingId = Catalog.NULL_INDEX;
    private final ClientProxy proxy;
    private final Image image;
    private final Catalog index;
    private final ImageRecorder.Builder builder;

    private ImageRecorder recorder;

    private State state = State.INIT;

    RecordingSession(
        final ClientProxy proxy,
        final Catalog index,
        final Image image,
        final ImageRecorder.Builder builder)
    {
        this.proxy = proxy;
        this.image = image;
        this.index = index;
        this.builder = builder;
    }

    public void abort()
    {
        this.state = State.CLOSING;
    }

    public int doWork()
    {
        int workDone = 0;

        if (state == State.INIT)
        {
            workDone += init();
        }

        if (state == State.ARCHIVING)
        {
            workDone += archive();
        }

        if (state == State.CLOSING)
        {
            workDone += close();
        }

        return workDone;
    }

    private int init()
    {
        final Subscription subscription = image.subscription();
        final int streamId = subscription.streamId();
        final String channel = subscription.channel();
        final int sessionId = image.sessionId();
        final String source = image.sourceIdentity();
        final int termBufferLength = image.termBufferLength();

        final int imageInitialTermId = image.initialTermId();


        ImageRecorder recorder = null;
        try
        {
            recordingId = index.addNewRecording(
                source,
                sessionId,
                channel,
                streamId,
                termBufferLength,
                imageInitialTermId,
                this,
                builder.recordingFileLength());

            proxy.recordingStarted(
                recordingId,
                source,
                sessionId,
                channel,
                streamId);

            recorder = builder
                .recordingId(recordingId)
                .termBufferLength(termBufferLength)
                .source(source)
                .sessionId(sessionId)
                .channel(channel)
                .streamId(streamId)
                .build();
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }

        this.recorder = recorder;
        this.state = State.ARCHIVING;
        return 1;
    }

    int recordingId()
    {
        return recordingId;
    }

    private int close()
    {
        try
        {
            if (recorder != null)
            {
                recorder.stop();
                index.updateCatalogFromMeta(recordingId, recorder.metaDataBuffer());
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            CloseHelper.quietClose(recorder);
            proxy.recordingStopped(recordingId);
            this.state = State.DONE;
        }

        return 1;
    }

    private int archive()
    {
        int workCount = 1;
        try
        {
            workCount = image.rawPoll(recorder, recorder.segmentFileLength());
            if (workCount != 0)
            {
                proxy.recordingProgress(
                    recorder.recordingId(),
                    recorder.initialTermId(),
                    recorder.initialTermOffset(),
                    recorder.lastTermId(),
                    recorder.lastTermOffset());
            }

            if (image.isClosed())
            {
                state = State.CLOSING;
            }
        }
        catch (final Exception ex)
        {
            state = State.CLOSING;
            LangUtil.rethrowUnchecked(ex);
        }

        return workCount;
    }

    public boolean isDone()
    {
        return state == State.DONE;
    }

    public void remove(final ArchiveConductor conductor)
    {
        index.removeRecordingSession(recordingId);
    }

    ByteBuffer metaDataBuffer()
    {
        return recorder.metaDataBuffer();
    }
}

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
import org.agrona.concurrent.EpochClock;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Consumes an {@link Image} and archives data into file using {@link StreamInstanceArchiveWriter}.
 */
class ImageArchivingSession implements ArchiverConductor.Session
{

    private enum State
    {
        ARCHIVING, CLOSING, DONE
    }

    private final int streamInstanceId;
    private final ArchiverProtocolProxy proxy;
    private final Image image;
    private final ArchiveIndex index;
    private final StreamInstanceArchiveWriter writer;

    private State state = State.ARCHIVING;

    ImageArchivingSession(
        final ArchiverProtocolProxy proxy,
        final ArchiveIndex index,
        final File archiveFolder,
        final Image image,
        final EpochClock epochClock)
    {
        this.proxy = proxy;
        this.image = image;

        final Subscription subscription = image.subscription();
        final int streamId = subscription.streamId();
        final String channel = subscription.channel();
        final int sessionId = image.sessionId();
        final String source = image.sourceIdentity();
        final int termBufferLength = image.termBufferLength();

        final int imageInitialTermId = image.initialTermId();
        this.index = index;
        streamInstanceId = index.addNewStreamInstance(
            new StreamInstance(source, sessionId, channel, streamId),
            termBufferLength,
            imageInitialTermId);

        proxy.notifyArchiveStarted(
            streamInstanceId,
            source,
            sessionId,
            channel,
            streamId);


        try
        {
            this.writer = new StreamInstanceArchiveWriter(
                archiveFolder,
                epochClock,
                streamInstanceId,
                termBufferLength,
                imageInitialTermId,
                new StreamInstance(source, sessionId, channel, streamId));

        }
        catch (Exception e)
        {
            close();
            LangUtil.rethrowUnchecked(e);
            // the next line is to keep compiler happy with regards to final fields init
            throw new RuntimeException();
        }
    }

    public void abort()
    {
        this.state = State.CLOSING;
    }

    public int doWork()
    {
        int workDone = 0;
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

    int streamInstanceId()
    {
        return writer.streamInstanceId();
    }

    private int close()
    {
        try
        {
            if (writer != null)
            {
                writer.stop();
                index.updateIndexFromMeta(streamInstanceId, writer.metaDataBuffer());
            }
        }
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
        finally
        {
            CloseHelper.quietClose(writer);
            proxy.notifyArchiveStopped(streamInstanceId);
            this.state = State.DONE;
        }
        return 1;
    }

    private int archive()
    {
        try
        {
            // TODO: add CRC as option, per fragment, use session id to store CRC
            final int delta = this.image.rawPoll(writer, ArchiveFileUtil.ARCHIVE_FILE_SIZE);
            if (delta != 0)
            {
                this.proxy.notifyArchiveProgress(
                    writer.streamInstanceId(),
                    writer.initialTermId(),
                    writer.initialTermOffset(),
                    writer.lastTermId(),
                    writer.lastTermOffset());
            }
            if (this.image.isClosed())
            {
                this.state = State.CLOSING;
            }
            return delta;
        }
        catch (Exception e)
        {
            this.state = State.CLOSING;
            LangUtil.rethrowUnchecked(e);
        }
        return 1;
    }

    public boolean isDone()
    {
        return state == State.DONE;
    }

    public void remove(final ArchiverConductor conductor)
    {
        conductor.removeArchivingSession(streamInstanceId);
    }

    ByteBuffer metaDataBuffer()
    {
        return writer.metaDataBuffer();
    }
}

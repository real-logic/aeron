/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochClock;

/**
 * Consumes an {@link Image} and archives data into file using {@link StreamInstanceArchiveWriter}.
 * TODO: refactor out of State pattern
 */
class ImageArchivingSession
{

    private enum State
    {
        ARCHIVING, CLOSING, DONE
    }

    private final int streamInstanceId;
    private final ArchiverConductor archiverConductor;
    private final Image image;
    private final StreamInstanceArchiveWriter writer;

    private State state = State.ARCHIVING;

    ImageArchivingSession(final ArchiverConductor archiverConductor, final Image image, final EpochClock epochClock)
    {
        this.archiverConductor = archiverConductor;
        this.image = image;

        final Subscription subscription = image.subscription();
        final int streamId = subscription.streamId();
        final String channel = subscription.channel();
        final int sessionId = image.sessionId();
        final String source = image.sourceIdentity();
        streamInstanceId = archiverConductor.notifyArchiveStarted(source, sessionId, channel, streamId);
        final int termBufferLength = image.termBufferLength();


        try
        {
            this.writer = new StreamInstanceArchiveWriter(
                archiverConductor.archiveFolder(),
                epochClock,
                streamInstanceId,
                termBufferLength,
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

    void abortArchive()
    {
        state(State.CLOSING);
    }

    int doWork()
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

    Image image()
    {
        return image;
    }

    State state()
    {
        return state;
    }

    private void state(final State state)
    {
        this.state = state;
    }

    int streamInstanceId()
    {
        return writer.streamInstanceId();
    }

    private int close()
    {
        if (writer != null)
        {
            writer.close();
        }
        archiverConductor.notifyArchiveStopped(streamInstanceId);
        state(State.DONE);
        return 1;
    }

    private int archive()
    {
        final StreamInstanceArchiveWriter writer = this.writer;
        try
        {
            final int delta = this.image.rawPoll(writer, ArchiveFileUtil.ARCHIVE_FILE_SIZE);
            if (delta != 0)
            {
                this.archiverConductor.notifyArchiveProgress(
                    writer.streamInstanceId(),
                    writer.initialTermId(),
                    writer.initialTermOffset(),
                    writer.lastTermId(),
                    writer.lastTermOffset());
            }
            if (this.image.isClosed())
            {
                this.state(State.CLOSING);
            }
            return delta;
        }
        catch (Exception e)
        {
            this.state(State.CLOSING);
            LangUtil.rethrowUnchecked(e);
        }
        return 1;
    }

    boolean isDone()
    {
        return state == State.DONE;
    }
}

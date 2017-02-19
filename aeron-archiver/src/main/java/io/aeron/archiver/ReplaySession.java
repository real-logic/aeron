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
import io.aeron.archiver.messages.ArchiveMetaFileFormatDecoder;
import org.agrona.*;

import java.io.*;

class ReplaySession
{
    enum State
    {
        INIT, REPLAY, CLOSE, DONE
    }

    private final StreamInstance streamInstance;

    // replay boundaries
    private final int fromTermId;
    private final int fromTermOffset;
    private final long replayLength;

    // 2 way comms setup
    private final Publication control;
    private final Publication replay;
    private final Image image;

    private final ArchiverConductor archiverConductor;

    private State state = State.INIT;
    private int streamInstanceId;
    private ArchiveDataChunkReadingCursor cursor;

    ReplaySession(
        final StreamInstance streamInstance,
        final int fromTermId,
        final int fromTermOffset,
        final long replayLength,
        final Publication replay,
        final Publication control,
        final Image image,
        final ArchiverConductor archiverConductor)
    {
        this.streamInstance = streamInstance;

        this.fromTermId = fromTermId;
        this.fromTermOffset = fromTermOffset;
        this.replayLength = replayLength;

        this.control = control;
        this.replay = replay;
        this.image = image;
        this.archiverConductor = archiverConductor;
    }

    int doWork()
    {
        if (state == State.REPLAY)
        {
            return replay();
        }
        else if (state == State.INIT)
        {
            return init();
        }
        else if (state == State.CLOSE)
        {
            return close();
        }
        return 0;
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

    void abortReplay()
    {
        state(State.CLOSE);
    }

    private int init()
    {
        // wait until outgoing publications are in place
        if (!replay.isConnected() || !control.isConnected())
        {
            // TODO: introduce some timeout mechanism here to prevent stale requests linger
            return 0;
        }
        // failure before SETUP is not reportable...
        else if (replay.isClosed() || control.isClosed())
        {
            state(State.CLOSE);
            return 0;
        }

        streamInstanceId = archiverConductor.getStreamInstanceId(streamInstance);
        final String archiveMetaFileName = ArchiveFileUtil.archiveMetaFileName(streamInstanceId);
        final File archiveMetaFile = new File(archiverConductor.archiveFolder(), archiveMetaFileName);
        if (!archiveMetaFile.exists())
        {
            final String err = archiveMetaFile.getAbsolutePath() + " not found";
            return closeOnErr(null, err);
        }

        final ArchiveMetaFileFormatDecoder archiveMetaFileFormatDecoder;
        try
        {
            archiveMetaFileFormatDecoder = ArchiveFileUtil.archiveMetaFileFormatDecoder(archiveMetaFile);
        }
        catch (IOException e)
        {
            final String err = archiveMetaFile.getAbsolutePath() + " : failed to map";
            return closeOnErr(e, err);
        }

        final int initialTermId = archiveMetaFileFormatDecoder.initialTermId();
        final int lastTermId = archiveMetaFileFormatDecoder.lastTermId();
        final int termId = fromTermId;
        if ((initialTermId <= lastTermId && (termId < initialTermId || termId > lastTermId)) ||
            (initialTermId > lastTermId && (termId > initialTermId || termId < lastTermId)))
        {
            return closeOnErr(null, "Requested term (" +
                                    termId + ") out of archive range [" +
                                    initialTermId + "," + lastTermId + "]");
        }
        // TODO: cover termOffset edge cases: range[0, termBufferLength],
        // TODO: or [initialOffset, termBufferLength] if first term or, [0, lastOffset] if lastTerm.

        // TODO: what should we do if the length exceeds range? error or replay what's available?

        // TODO: open ended replay


        final int termBufferLength = archiveMetaFileFormatDecoder.termBufferLength();
        cursor = new ArchiveDataChunkReadingCursor(streamInstanceId,
                                                   archiverConductor.archiveFolder(),
                                                   initialTermId,
                                                   termBufferLength,
                                                   fromTermId,
                                                   fromTermOffset,
                                                   replayLength);
        // plumbing is secured, we can kick off the replay
        archiverConductor.sendResponse(control, null);
        state(State.REPLAY);
        return 1;
    }

    private int closeOnErr(final Throwable e,
                            final String err)
    {
        state(State.CLOSE);
        if (control.isConnected())
        {
            archiverConductor.sendResponse(control, err);
        }
        if (e != null)
        {
            LangUtil.rethrowUnchecked(e);
        }
        return 0;
    }

    private int replay()
    {
        final int mtu = replay.maxPayloadLength();

        final ArchiverConductor conductor = archiverConductor;
        try
        {
            int readBytes = cursor.readChunk((b, offset, length) ->
            {
                final long result = replay.offer(b, offset, length);
                if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED)
                {
                    throw new IllegalStateException();
                }
                return result >= 0;
            }, mtu);
            if (cursor.isDone())
            {
                // TODO: This is actually premature closure of publication
                state(State.CLOSE);
            }
            return readBytes;
        }
        catch (Exception e)
        {
            return closeOnErr(e, "Cursor read failed");
        }
    }

    private int close()
    {
        CloseHelper.quietClose(control);
        CloseHelper.quietClose(replay);
        CloseHelper.quietClose(cursor);
        state(State.DONE);
        return 1;
    }
}

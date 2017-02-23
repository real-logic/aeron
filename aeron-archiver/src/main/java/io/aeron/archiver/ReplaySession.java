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
import io.aeron.archiver.messages.*;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;

/**
 * A replay session with a client which works through the required request response flow and streaming of archived data.
 * The {@link ArchiverConductor} will initiate a session on receiving a ReplayRequest
 * (see {@link io.aeron.archiver.messages.ReplayRequestDecoder}). The session will:
 * <ul>
 *     <li>Establish a reply {@link Publication} with the initiator(or someone else possibly) </li>
 *     <li>Validate request parameters and respond with error, or OK message(see {@link ArchiverResponseDecoder})</li>
 *     <li>Stream archived data into reply {@link Publication}</li>
 *     <li>Successfully terminate the stream (see {@link ReplayFinishedDecoder})</li>
 * </ul>
 * TODO: implement open ended replay
 */
class ReplaySession
{
    static final long REPLAY_DATA_HEADER;

    static
    {
        final MessageHeaderEncoder encoder = new MessageHeaderEncoder();
        encoder.wrap(new UnsafeBuffer(new byte[8]), 0);
        encoder.schemaId(ReplayDataDecoder.SCHEMA_ID);
        encoder.version(ReplayDataDecoder.SCHEMA_VERSION);
        encoder.blockLength(ReplayDataDecoder.BLOCK_LENGTH);
        encoder.templateId(ReplayDataDecoder.TEMPLATE_ID);
        REPLAY_DATA_HEADER = encoder.buffer().getLong(0);
    }

    enum State
    {
        INIT,
        REPLAY,
        CLOSE,
        DONE
    }

    // replay boundaries
    private final int streamInstanceId;
    private final int fromTermId;
    private final int fromTermOffset;
    private final long replayLength;

    private final Publication reply;
    private final Image image;

    private final ArchiverConductor archiverConductor;
    private final BufferClaim bufferClaim = new BufferClaim();


    private State state = State.INIT;
    private StreamInstanceArchiveChunkReader cursor;

    ReplaySession(
        final int streamInstanceId,
        final int fromTermId,
        final int fromTermOffset,
        final long replayLength,
        final Publication reply,
        final Image image,
        final ArchiverConductor archiverConductor)
    {
        this.streamInstanceId = streamInstanceId;

        this.fromTermId = fromTermId;
        this.fromTermOffset = fromTermOffset;
        this.replayLength = replayLength;

        this.reply = reply;
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
        if (reply.isClosed())
        {
            state(State.CLOSE);
            return 0;
        }

        // wait until outgoing publications are in place
        if (!reply.isConnected())
        {
            // TODO: introduce some timeout mechanism here to prevent stale requests linger
            return 0;
        }

        final String archiveMetaFileName = ArchiveFileUtil.archiveMetaFileName(streamInstanceId);
        final File archiveMetaFile = new File(archiverConductor.archiveFolder(), archiveMetaFileName);
        if (!archiveMetaFile.exists())
        {
            final String err = archiveMetaFile.getAbsolutePath() + " not found";
            return closeOnErr(null, err);
        }

        final ArchiveMetaFileFormatDecoder metaData;
        try
        {
            metaData = ArchiveFileUtil.archiveMetaFileFormatDecoder(archiveMetaFile);
        }
        catch (IOException e)
        {
            final String err = archiveMetaFile.getAbsolutePath() + " : failed to map";
            return closeOnErr(e, err);
        }

        final int initialTermId = metaData.initialTermId();
        final int initialTermOffset = metaData.initialTermOffset();

        final int lastTermId = metaData.lastTermId();
        final int lastTermOffset = metaData.lastTermOffset();
        final int termBufferLength = metaData.termBufferLength();

        // Note: when debugging this may cause a crash as the debugger might try to call metaData.toString after unmap
        IoUtil.unmap(metaData.buffer().byteBuffer());

        final int replayEndTermId = (int) (fromTermId + (replayLength / termBufferLength));
        final int replayEndTermOffset = (int) ((replayLength + fromTermOffset) % termBufferLength);

        if (fromTermOffset >= termBufferLength || fromTermOffset < 0 ||
            !isTermIdInRange(fromTermId, initialTermId, lastTermId) ||
            !isTermOffsetInRange(initialTermId,
                                 initialTermOffset,
                                 lastTermId,
                                 lastTermOffset,
                                 fromTermId,
                                 fromTermOffset) ||
            !isTermIdInRange(replayEndTermId, initialTermId, lastTermId) ||
            !isTermOffsetInRange(initialTermId,
                                 initialTermOffset,
                                 lastTermId,
                                 lastTermOffset,
                                 replayEndTermId,
                                 replayEndTermOffset))
        {
            return closeOnErr(null, "Requested replay is out of archive range [(" +
                                    initialTermId + "," + initialTermOffset + "),(" +
                                    lastTermId + "," + lastTermOffset + ")]");
        }

        try
        {
            cursor = new StreamInstanceArchiveChunkReader(streamInstanceId,
                                                          archiverConductor.archiveFolder(),
                                                          initialTermId,
                                                          termBufferLength,
                                                          fromTermId,
                                                          fromTermOffset,
                                                          replayLength);
        }
        catch (IOException e)
        {
            return closeOnErr(e, "Failed to open archive cursor");
        }
        // plumbing is secured, we can kick off the replay
        archiverConductor.sendResponse(reply, null);
        state(State.REPLAY);
        return 1;
    }

    private static boolean isTermOffsetInRange(final int initialTermId,
                                               final int initialTermOffset,
                                               final int lastTermId,
                                               final int lastTermOffset,
                                               final int termId,
                                               final int termOffset)
    {
        return (initialTermId == termId && termOffset >= initialTermOffset) ||
            (lastTermId == termId && termOffset <= lastTermOffset);
    }

    private static boolean isTermIdInRange(final int term, final int start, final int end)
    {
        if (start <= end)
        {
            return term >= start && term <= end;
        }
        else
        {
            return term >= start || term <= end;
        }
    }

    private int closeOnErr(final Throwable e, final String err)
    {
        state(State.CLOSE);
        if (reply.isConnected())
        {
            archiverConductor.sendResponse(reply, err);
        }
        if (e != null)
        {
            LangUtil.rethrowUnchecked(e);
        }
        return 0;
    }

    private int replay()
    {
        final int mtu = reply.maxPayloadLength() - MessageHeaderDecoder.ENCODED_LENGTH;

        try
        {
            final int readBytes = cursor.readChunk(this::handleChunks, mtu);
            if (cursor.isDone())
            {
                state(State.CLOSE);
            }
            return readBytes;
        }
        catch (Exception e)
        {
            return closeOnErr(e, "Cursor read failed");
        }
    }

    private boolean handleChunks(final UnsafeBuffer chunkBuffer, final int chunkOffset, final int chunkLength)
    {
        final long result = reply.tryClaim(chunkLength + MessageHeaderDecoder.ENCODED_LENGTH,
                                           bufferClaim);
        if (result > 0)
        {
            try
            {
                final MutableDirectBuffer buffer = bufferClaim.buffer();
                final int offset = bufferClaim.offset();
                buffer.putLong(offset, REPLAY_DATA_HEADER);
                buffer.putBytes(offset + 8, chunkBuffer, chunkOffset, chunkLength);
            }
            finally
            {
                bufferClaim.commit();
            }
        }
        else if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED)
        {
            throw new IllegalStateException("Reply publication to replay requestor has shutdown mid-replay");
        }
        return result >= 0;
    }

    private int close()
    {
        // TODO: how do we gracefully timeout or terminate this? need to add a LINGER state etc.
        CloseHelper.quietClose(reply);
        CloseHelper.quietClose(cursor);
        state(State.DONE);
        return 1;
    }
}

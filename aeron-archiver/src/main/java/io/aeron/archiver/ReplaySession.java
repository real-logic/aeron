/*
 * Copyright 2014-2017 Real Logic Ltd.
 *
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
 */
package io.aeron.archiver;

import io.aeron.*;
import io.aeron.archiver.codecs.*;
import io.aeron.logbuffer.ExclusiveBufferClaim;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.*;

import java.io.*;

import static io.aeron.logbuffer.FrameDescriptor.frameFlags;
import static io.aeron.logbuffer.FrameDescriptor.frameType;
import static io.aeron.protocol.DataHeaderFlyweight.RESERVED_VALUE_OFFSET;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * A replay session with a client which works through the required request response flow and streaming of recorded data.
 * The {@link ArchiveConductor} will initiate a session on receiving a ReplayRequest
 * (see {@link io.aeron.archiver.codecs.ReplayRequestDecoder}). The session will:
 * <ul>
 * <li>Validate request parameters and respond with error, or OK message
 * (see {@link io.aeron.archiver.codecs.ControlResponseDecoder})</li>
 * <li>Stream recorded data into the replayPublication {@link Publication}</li>
 * </ul>
 */
class ReplaySession
    implements Session, RecordingFragmentReader.SimplifiedControlledPoll
{
    enum State
    {
        INIT, REPLAY, LINGER, INACTIVE, CLOSED
    }

    private static final int REPLAY_SEND_BATCH_SIZE = 8;
    static final long LINGER_LENGTH_MS = 1000;

    private final long recordingId;
    private final long replayPosition;
    private final long replayLength;

    private final Replayer replayer;
    private final Publication controlPublication;
    private final ControlSessionProxy controlSessionProxy;

    private final File archiveDir;
    private final ExclusiveBufferClaim bufferClaim = new ExclusiveBufferClaim();

    private State state = State.INIT;
    private RecordingFragmentReader cursor;
    private final long replaySessionId;
    private final long correlationId;
    private final EpochClock epochClock;
    private final String replayChannel;
    private final int replayStreamId;

    private ExclusivePublication replayPublication;
    private int mtuLength;
    private int termBufferLength;
    private int initialTermId;
    private long lingerSinceMs;

    ReplaySession(
        final long recordingId,
        final long replayPosition,
        final long replayLength,
        final Replayer replayer,
        final Publication controlPublication,
        final File archiveDir,
        final ControlSessionProxy controlSessionProxy,
        final long replaySessionId,
        final long correlationId,
        final EpochClock epochClock,
        final String replayChannel,
        final int replayStreamId)
    {
        this.recordingId = recordingId;

        this.replayPosition = replayPosition;
        this.replayLength = replayLength;
        this.replayer = replayer;

        this.controlPublication = controlPublication;
        this.archiveDir = archiveDir;
        this.controlSessionProxy = controlSessionProxy;
        this.replaySessionId = replaySessionId;
        this.correlationId = correlationId;
        this.epochClock = epochClock;
        this.lingerSinceMs = epochClock.time();

        this.replayChannel = replayChannel;
        this.replayStreamId = replayStreamId;
    }

    public int doWork()
    {
        int workDone = 0;
        if (state == State.REPLAY)
        {
            workDone += replay();
        }
        else if (state == State.INIT)
        {
            workDone += init();
        }
        else if (state == State.LINGER)
        {
            workDone += linger();
        }

        if (state == State.INACTIVE)
        {
            workDone += close();
        }

        return workDone;
    }

    public void abort()
    {
        if (controlPublication.isConnected())
        {
            controlSessionProxy.sendReplayAborted(
                controlPublication,
                correlationId,
                replaySessionId,
                replayPublication.position());
        }

        this.state = State.INACTIVE;
    }

    public boolean isDone()
    {
        return state == State.CLOSED;
    }

    public boolean onFragment(final UnsafeBuffer termBuffer, final int offset, final int length)
    {
        if (isDone())
        {
            return false;
        }

        final long result = replayPublication.tryClaim(length, bufferClaim);
        if (result > 0)
        {
            try
            {
                final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
                final UnsafeBuffer publicationBuffer = (UnsafeBuffer)bufferClaim.buffer();

                bufferClaim.flags(frameFlags(termBuffer, frameOffset));
                bufferClaim.headerType(frameType(termBuffer, frameOffset));
                bufferClaim.reservedValue(termBuffer.getLong(frameOffset + RESERVED_VALUE_OFFSET, LITTLE_ENDIAN));

                publicationBuffer.putBytes(bufferClaim.offset(), termBuffer, offset, length);
            }
            finally
            {
                bufferClaim.commit();
            }

            return true;
        }
        else if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED)
        {
            closeOnError(null, "Replay stream has been shutdown mid-replay");
        }

        return false;
    }

    State state()
    {
        return state;
    }

    private int linger()
    {
        if (isLingerDone())
        {
            this.state = State.INACTIVE;
        }

        return 0;
    }

    private boolean isLingerDone()
    {
        return epochClock.time() - LINGER_LENGTH_MS > lingerSinceMs;
    }

    private int init()
    {
        if (cursor == null)
        {
            final String recordingMetaFileName = ArchiveUtil.recordingMetaFileName(recordingId);
            final File recordingMetaFile = new File(archiveDir, recordingMetaFileName);
            if (!recordingMetaFile.exists())
            {
                return closeOnError(null, recordingMetaFile.getAbsolutePath() + " not found");
            }

            final RecordingDescriptorDecoder metaData;
            try
            {
                metaData = ArchiveUtil.loadRecordingDescriptor(recordingMetaFile);
            }
            catch (final IOException ex)
            {
                return closeOnError(ex, recordingMetaFile.getAbsolutePath() + " : failed to map");
            }

            final long joiningPosition = metaData.joiningPosition();
            final long lastPosition = metaData.lastPosition();
            mtuLength = metaData.mtuLength();
            termBufferLength = metaData.termBufferLength();
            initialTermId = metaData.initialTermId();

            if (this.replayPosition < joiningPosition)
            {
                return closeOnError(null, "Requested replay start position(=" + replayPosition +
                    ") is less than recording joining position(=" + joiningPosition + ")");
            }
            final long toPosition = this.replayLength + replayPosition;
            if (toPosition > lastPosition)
            {
                return closeOnError(null, "Requested replay end position(=" + toPosition +
                    ") is more than recording end position(=" + lastPosition + ")");
            }

            try
            {
                cursor = new RecordingFragmentReader(recordingId, archiveDir, replayPosition, replayLength);
            }
            catch (final IOException ex)
            {
                return closeOnError(ex, "Failed to open cursor for a recording");
            }
        }

        if (replayPublication == null)
        {
            try
            {
                // TODO: if we want a NoOp client lock in the DEDICATED mode this needs to be done on the replayer
                replayPublication = replayer.newReplayPublication(
                    replayChannel,
                    replayStreamId,
                    cursor.fromPosition(),
                    mtuLength,
                    initialTermId,
                    termBufferLength);
            }
            catch (final Exception ex)
            {
                return closeOnError(ex, "Failed to create replay publication");
            }
        }

        if (!replayPublication.isConnected())
        {
            if (isLingerDone())
            {
                return closeOnError(null, "No subscription to replay publication has been made");
            }

            return 0;
        }

        controlSessionProxy.sendOkResponse(controlPublication, correlationId);
        this.state = State.REPLAY;

        return 1;
    }

    private int closeOnError(final Throwable e, final String errorMessage)
    {
        this.state = State.INACTIVE;
        if (controlPublication.isConnected())
        {
            controlSessionProxy.sendError(controlPublication, ControlResponseCode.ERROR, errorMessage, correlationId);
        }

        if (e != null)
        {
            LangUtil.rethrowUnchecked(e);
        }

        return 0;
    }

    private int replay()
    {
        try
        {
            final int polled = cursor.controlledPoll(this, REPLAY_SEND_BATCH_SIZE);
            if (cursor.isDone())
            {
                lingerSinceMs = epochClock.time();
                this.state = State.LINGER;
            }

            return polled;
        }
        catch (final Exception ex)
        {
            return closeOnError(ex, "Cursor read failed");
        }
    }

    private int close()
    {
        // TODO: if we want a NoOp client lock in the DEDICATED mode this needs to be done on the replayer
        CloseHelper.close(replayPublication);
        CloseHelper.close(cursor);
        this.state = State.CLOSED;

        return 1;
    }

    public long sessionId()
    {
        return replaySessionId;
    }
}

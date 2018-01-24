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
package io.aeron.archive;

import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.logbuffer.ExclusiveBufferClaim;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;

import static io.aeron.logbuffer.FrameDescriptor.frameFlags;
import static io.aeron.logbuffer.FrameDescriptor.frameType;
import static io.aeron.protocol.DataHeaderFlyweight.RESERVED_VALUE_OFFSET;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * A replay session with a client which works through the required request response flow and streaming of recorded data.
 * The {@link ArchiveConductor} will initiate a session on receiving a ReplayRequest
 * (see {@link io.aeron.archive.codecs.ReplayRequestDecoder}). The session will:
 * <ul>
 * <li>Validate request parameters and respond with appropriate error if unable to replay </li>
 * <li>Wait for replay subscription to connect to the requested replay publication. If no subscription appears within
 * {@link #CONNECT_TIMEOUT_MS} the session will terminate and respond will error.</li>
 * <li>Once the replay publication is connected send an OK response to control client</li>
 * <li>Stream recorded data into the replayPublication {@link ExclusivePublication}</li>
 * <li>If the replay is aborted part way through, send a ReplayAborted message and terminate.</li>
 * </ul>
 */
class ReplaySession implements Session, SimpleFragmentHandler
{
    enum State
    {
        INIT, REPLAY, INACTIVE, CLOSED
    }

    /**
     * Timeout within which a replay connection needs to be established.
     */
    static final long CONNECT_TIMEOUT_MS = 5000;

    private static final int REPLAY_FRAGMENT_LIMIT = Archive.Configuration.replayFragmentLimit();

    private long connectDeadlineMs;
    private final long correlationId;
    private final ExclusiveBufferClaim bufferClaim = new ExclusiveBufferClaim();
    private final ExclusivePublication replayPublication;
    private final RecordingFragmentReader cursor;
    private ControlResponseProxy threadLocalControlResponseProxy;
    private final ControlSession controlSession;
    private final EpochClock epochClock;
    private State state = State.INIT;

    @SuppressWarnings("ConstantConditions")
    ReplaySession(
        final long replayPosition,
        final long replayLength,
        final Catalog catalog,
        final ControlSession controlSession,
        final File archiveDir,
        final ControlResponseProxy threadLocalControlResponseProxy,
        final long correlationId,
        final EpochClock epochClock,
        final ExclusivePublication replayPublication,
        final RecordingSummary recordingSummary,
        final Counter recordingPosition)
    {
        this.controlSession = controlSession;
        this.threadLocalControlResponseProxy = threadLocalControlResponseProxy;
        this.correlationId = correlationId;
        this.epochClock = epochClock;
        this.replayPublication = replayPublication;

        RecordingFragmentReader cursor = null;
        try
        {
            cursor = new RecordingFragmentReader(
                catalog,
                recordingSummary,
                archiveDir,
                replayPosition,
                replayLength,
                recordingPosition);
        }
        catch (final Exception ex)
        {
            closeOnError(ex, "failed to open cursor on a recording because: " + ex.getMessage());
        }

        this.cursor = cursor;

        controlSession.sendOkResponse(correlationId, replayPublication.sessionId(), threadLocalControlResponseProxy);

        connectDeadlineMs = epochClock.time() + CONNECT_TIMEOUT_MS;
    }

    public void close()
    {
        state = State.CLOSED;

        if (null != cursor)
        {
            cursor.close();
        }

        CloseHelper.close(replayPublication);
    }

    public long sessionId()
    {
        return replayPublication.sessionId();
    }

    public int doWork()
    {
        int workCount = 0;

        switch (state)
        {
            case INIT:
                workCount += init();
                break;

            case REPLAY:
                workCount += replay();
                break;
        }

        return workCount;
    }

    public void abort()
    {
        CloseHelper.close(replayPublication);
    }

    public boolean isDone()
    {
        return state == State.INACTIVE;
    }

    public boolean onFragment(final UnsafeBuffer buffer, final int offset, final int length)
    {
        if (state != State.REPLAY)
        {
            return false;
        }

        final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
        final int frameType = frameType(buffer, frameOffset);

        final long result = frameType == FrameDescriptor.PADDING_FRAME_TYPE ?
            replayPublication.appendPadding(length) :
            replayFrame(buffer, offset, length, frameOffset);

        if (result > 0)
        {
            return true;
        }
        else if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED)
        {
            closeOnError(null, "replay stream has been shutdown mid-replay");
        }

        return false;
    }

    State state()
    {
        return state;
    }

    void setThreadLocalControlResponseProxy(final ControlResponseProxy proxy)
    {
        threadLocalControlResponseProxy = proxy;
    }

    private int replay()
    {
        int workDone = 0;
        try
        {
            workDone = cursor.controlledPoll(this, REPLAY_FRAGMENT_LIMIT);
            if (cursor.isDone())
            {
                state = State.INACTIVE;
            }
        }
        catch (final Exception ex)
        {
            closeOnError(ex, "cursor read failed");
        }

        return workDone;
    }

    private long replayFrame(final UnsafeBuffer buffer, final int offset, final int length, final int frameOffset)
    {
        final long result = replayPublication.tryClaim(length, bufferClaim);
        if (result > 0)
        {
            bufferClaim
                .flags(frameFlags(buffer, frameOffset))
                .reservedValue(buffer.getLong(frameOffset + RESERVED_VALUE_OFFSET, LITTLE_ENDIAN))
                .buffer().putBytes(bufferClaim.offset(), buffer, offset, length);

            bufferClaim.commit();
        }

        return result;
    }

    private int init()
    {
        if (!replayPublication.isConnected())
        {
            if (epochClock.time() > connectDeadlineMs)
            {
                closeOnError(null, "no connection established for replay");
            }

            return 0;
        }

        state = State.REPLAY;

        return 1;
    }

    private void closeOnError(final Throwable ex, final String errorMessage)
    {
        state = State.INACTIVE;
        CloseHelper.quietClose(replayPublication);

        if (null != cursor)
        {
            cursor.close();
        }

        if (!controlSession.isDone())
        {
            controlSession.sendResponse(
                correlationId,
                ControlResponseCode.ERROR,
                errorMessage,
                threadLocalControlResponseProxy);
        }

        if (ex != null)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}

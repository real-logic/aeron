/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.EnumSet;

import static io.aeron.archive.Archive.segmentFileIndex;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.RESERVED_VALUE_OFFSET;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;

/**
 * A replay session with a client which works through the required request response flow and streaming of recorded data.
 * The {@link ArchiveConductor} will initiate a session on receiving a ReplayRequest
 * (see {@link io.aeron.archive.codecs.ReplayRequestDecoder}).
 * <p>
 * The session will:
 * <ul>
 * <li>Validate request parameters and respond with appropriate error if unable to replay.</li>
 * <li>Wait for replay publication to connect to the subscriber. If no subscriber appears within
 * {@link Archive.Configuration#CONNECT_TIMEOUT_PROP_NAME} the session will terminate and respond with an error.</li>
 * <li>Once the replay publication is connected an OK response to control client will be sent.</li>
 * <li>Stream recorded data into the replayPublication {@link ExclusivePublication}.</li>
 * <li>If the replay is aborted part way through, send a ReplayAborted message and terminate.</li>
 * </ul>
 */
class ReplaySession implements Session, AutoCloseable
{
    enum State
    {
        INIT, REPLAY, INACTIVE, DONE
    }

    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(READ);
    private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute[0];
    private static final int REPLAY_FRAGMENT_LIMIT = Archive.Configuration.replayFragmentLimit();

    private final long connectDeadlineMs;
    private final long correlationId;
    private final long sessionId;

    private final long startPosition;
    private long stopPosition;
    private long replayPosition;
    private long replayLimit;
    private int termOffset;
    private int termBaseSegmentOffset;
    private int segmentFileIndex;
    private final int initialTermId;
    private final int streamId;

    private final BufferClaim bufferClaim = new BufferClaim();
    private final ExclusivePublication replayPublication;
    private final ControlSession controlSession;
    private final EpochClock epochClock;

    private final File archiveDir;
    private final long recordingId;
    private final int segmentLength;
    private final int termLength;

    private final Catalog catalog;
    private final Counter recordingPosition;
    private final UnsafeBuffer termBuffer = new UnsafeBuffer();
    private MappedByteBuffer mappedSegmentBuffer;

    private State state = State.INIT;
    private String errorMessage = null;
    private volatile boolean isAborted;

    ReplaySession(
        final long position,
        final long length,
        final long replaySessionId,
        final long connectTimeoutMs,
        final Catalog catalog,
        final ControlSession controlSession,
        final File archiveDir,
        final ControlResponseProxy controlResponseProxy,
        final long correlationId,
        final EpochClock epochClock,
        final ExclusivePublication replayPublication,
        final RecordingSummary recordingSummary,
        final Counter recordingPosition)
    {
        this.controlSession = controlSession;
        this.sessionId = replaySessionId;
        this.correlationId = correlationId;
        this.recordingId = recordingSummary.recordingId;
        this.segmentLength = recordingSummary.segmentFileLength;
        this.termLength = recordingSummary.termBufferLength;
        this.initialTermId = recordingSummary.initialTermId;
        this.streamId = recordingSummary.streamId;
        this.epochClock = epochClock;
        this.archiveDir = archiveDir;
        this.replayPublication = replayPublication;
        this.recordingPosition = recordingPosition;
        this.catalog = catalog;
        this.startPosition = recordingSummary.startPosition;
        this.stopPosition = null == recordingPosition ? recordingSummary.stopPosition : recordingPosition.get();

        final long fromPosition = position == NULL_POSITION ? startPosition : position;
        final long maxLength = null == recordingPosition ? stopPosition - fromPosition : Long.MAX_VALUE - fromPosition;
        final long replayLength = length == AeronArchive.NULL_LENGTH ? maxLength : Math.min(length, maxLength);
        if (replayLength < 0)
        {
            close();
            final String msg = "replay recording " + recordingId + " - " + "length must be positive: " + replayLength;
            controlSession.attemptErrorResponse(correlationId, msg, controlResponseProxy);
            throw new IllegalArgumentException(msg);
        }

        if (null != recordingPosition)
        {
            final long currentPosition = recordingPosition.get();
            if (currentPosition < fromPosition)
            {
                close();
                final String msg = "replay recording " + recordingId + " - " +
                    fromPosition + " after current position of " + currentPosition;
                controlSession.attemptErrorResponse(correlationId, msg, controlResponseProxy);
                throw new IllegalArgumentException(msg);
            }
        }

        segmentFileIndex = segmentFileIndex(startPosition, fromPosition, segmentLength);
        replayPosition = fromPosition;
        replayLimit = fromPosition + replayLength;

        controlSession.sendOkResponse(correlationId, replaySessionId, controlResponseProxy);
        connectDeadlineMs = epochClock.time() + connectTimeoutMs;
    }

    public void close()
    {
        CloseHelper.close(replayPublication);
        closeRecordingSegment();
    }

    public long sessionId()
    {
        return sessionId;
    }

    public int doWork()
    {
        int workCount = 0;

        if (isAborted)
        {
            state = State.INACTIVE;
        }

        if (State.INIT == state)
        {
            workCount += init();
        }

        if (State.REPLAY == state)
        {
            workCount += replay();
        }

        if (State.INACTIVE == state)
        {
            closeRecordingSegment();
            state = State.DONE;
        }

        return workCount;
    }

    public void abort()
    {
        isAborted = true;
    }

    public boolean isDone()
    {
        return state == State.DONE;
    }

    long recordingId()
    {
        return recordingId;
    }

    State state()
    {
        return state;
    }

    void sendPendingError(final ControlResponseProxy controlResponseProxy)
    {
        if (null != errorMessage && !controlSession.isDone())
        {
            controlSession.attemptErrorResponse(correlationId, errorMessage, controlResponseProxy);
        }
    }

    private int init()
    {
        if (null == mappedSegmentBuffer)
        {
            final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
            final long startTermBasePosition = startPosition - (startPosition & (termLength - 1));
            final int segmentOffset = (int)(replayPosition - startTermBasePosition) & (segmentLength - 1);
            final int termId = ((int)(replayPosition >> positionBitsToShift) + initialTermId);

            openRecordingSegment();

            termOffset = (int)(replayPosition & (termLength - 1));
            termBaseSegmentOffset = segmentOffset - termOffset;
            termBuffer.wrap(mappedSegmentBuffer, termBaseSegmentOffset, termLength);

            if (replayPosition > startPosition &&
                replayPosition != stopPosition &&
                isInvalidHeader(termBuffer, streamId, termId, termOffset))
            {
                onError("replay recording " + recordingId + " - " +
                    replayPosition + " position not aligned to valid fragment");

                return 0;
            }
        }

        if (!replayPublication.isConnected())
        {
            if (epochClock.time() > connectDeadlineMs)
            {
                onError("no connection established for replay");
            }

            return 0;
        }

        state = State.REPLAY;

        return 1;
    }

    private int replay()
    {
        int fragments = 0;

        if (recordingPosition != null && replayPosition == stopPosition && noNewData(replayPosition, stopPosition))
        {
            return fragments;
        }

        while (replayPosition < stopPosition && fragments < REPLAY_FRAGMENT_LIMIT)
        {
            if (termOffset == termLength)
            {
                nextTerm();
            }

            final int frameOffset = termOffset;
            final UnsafeBuffer termBuffer = this.termBuffer;
            final int frameLength = FrameDescriptor.frameLength(termBuffer, frameOffset);
            final int frameType = FrameDescriptor.frameType(termBuffer, frameOffset);

            final int alignedLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
            final int dataOffset = frameOffset + DataHeaderFlyweight.HEADER_LENGTH;
            final int dataLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;

            long result = 0;
            if (frameType == HeaderFlyweight.HDR_TYPE_DATA)
            {
                final BufferClaim bufferClaim = this.bufferClaim;
                result = replayPublication.tryClaim(dataLength, bufferClaim);
                if (result > 0)
                {
                    bufferClaim
                        .flags(FrameDescriptor.frameFlags(termBuffer, frameOffset))
                        .reservedValue(termBuffer.getLong(frameOffset + RESERVED_VALUE_OFFSET, LITTLE_ENDIAN))
                        .putBytes(termBuffer, dataOffset, dataLength);

                    bufferClaim.commit();
                }
            }
            else if (frameType == HeaderFlyweight.HDR_TYPE_PAD)
            {
                result = replayPublication.appendPadding(dataLength);
            }

            if (result > 0)
            {
                replayPosition += alignedLength;
                termOffset += alignedLength;
                fragments++;

                if (replayPosition >= replayLimit)
                {
                    state = State.INACTIVE;
                    break;
                }
            }
            else
            {
                if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED)
                {
                    onError("stream closed before replay is complete");
                }

                break;
            }
        }

        return fragments;
    }

    private void onError(final String errorMessage)
    {
        state = State.INACTIVE;
        this.errorMessage = errorMessage;
    }

    private boolean noNewData(final long replayPosition, final long oldStopPosition)
    {
        final long currentRecodingPosition = recordingPosition.get();
        final boolean hasRecordingStopped = recordingPosition.isClosed();
        final long newStopPosition = hasRecordingStopped ? catalog.stopPosition(recordingId) : currentRecodingPosition;

        if (hasRecordingStopped && newStopPosition < replayLimit)
        {
            replayLimit = newStopPosition;
        }

        if (replayPosition >= replayLimit)
        {
            state = State.INACTIVE;
        }
        else if (newStopPosition > oldStopPosition)
        {
            stopPosition = newStopPosition;
            return false;
        }

        return true;
    }

    private void nextTerm()
    {
        termOffset = 0;
        termBaseSegmentOffset += termLength;

        if (termBaseSegmentOffset == segmentLength)
        {
            closeRecordingSegment();
            segmentFileIndex++;
            openRecordingSegment();
            termBaseSegmentOffset = 0;
        }

        termBuffer.wrap(mappedSegmentBuffer, termBaseSegmentOffset, termLength);
    }

    private void closeRecordingSegment()
    {
        IoUtil.unmap(mappedSegmentBuffer);
        mappedSegmentBuffer = null;
    }

    private void openRecordingSegment()
    {
        final String segmentFileName = segmentFileName(recordingId, segmentFileIndex);
        final File segmentFile = new File(archiveDir, segmentFileName);

        if (!segmentFile.exists())
        {
            throw new IllegalArgumentException("failed to open recording segment file " + segmentFileName);
        }

        try (FileChannel channel = FileChannel.open(segmentFile.toPath(), FILE_OPTIONS, NO_ATTRIBUTES))
        {
            mappedSegmentBuffer = channel.map(READ_ONLY, 0, segmentLength);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    static boolean isInvalidHeader(final UnsafeBuffer buffer, final int streamId, final int termId, final int offset)
    {
        return
            DataHeaderFlyweight.termOffset(buffer, offset) != offset ||
            DataHeaderFlyweight.termId(buffer, offset) != termId ||
            DataHeaderFlyweight.streamId(buffer, offset) != streamId;
    }

}

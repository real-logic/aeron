/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.EnumSet;

import static io.aeron.archive.Archive.Configuration.MAX_BLOCK_LENGTH;
import static io.aeron.archive.Archive.segmentFileIndex;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.RESERVED_VALUE_OFFSET;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
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
 * <li>Stream recorded data into the publication {@link ExclusivePublication}.</li>
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

    private final long connectDeadlineMs;
    private final long correlationId;
    private final long sessionId;
    private final long recordingId;
    private final long startPosition;
    private long replayPosition;
    private long stopPosition;
    private long replayLimit;
    private int termOffset;
    private int termBaseSegmentOffset;
    private int segmentFileIndex;
    private final int streamId;
    private final int termLength;
    private final int segmentLength;

    private final BufferClaim bufferClaim = new BufferClaim();
    private final ExclusivePublication publication;
    private final ControlSession controlSession;
    private final EpochClock epochClock;
    private final File archiveDir;
    private final Catalog catalog;
    private final Counter recordingPosition;
    private final UnsafeBuffer replayBuffer;
    private FileChannel fileChannel;
    private File segmentFile;
    private State state = State.INIT;
    private String errorMessage = null;
    private volatile boolean isAborted;

    ReplaySession(
        final long position,
        final long length,
        final long replaySessionId,
        final long connectTimeoutMs,
        final long correlationId,
        final ControlSession controlSession,
        final ControlResponseProxy controlResponseProxy,
        final UnsafeBuffer replayBuffer,
        final Catalog catalog,
        final File archiveDir,
        final File initialSegmentFile,
        final EpochClock epochClock,
        final ExclusivePublication publication,
        final RecordingSummary recordingSummary,
        final Counter recordingPosition)
    {
        this.controlSession = controlSession;
        this.sessionId = replaySessionId;
        this.correlationId = correlationId;
        this.recordingId = recordingSummary.recordingId;
        this.segmentLength = recordingSummary.segmentFileLength;
        this.termLength = recordingSummary.termBufferLength;
        this.streamId = recordingSummary.streamId;
        this.epochClock = epochClock;
        this.archiveDir = archiveDir;
        this.segmentFile = initialSegmentFile;
        this.publication = publication;
        this.recordingPosition = recordingPosition;
        this.replayBuffer = replayBuffer;
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
            throw new ArchiveException(msg);
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
                throw new ArchiveException(msg);
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
        closeRecordingSegment();
        CloseHelper.close(publication);
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

        try
        {
            if (State.INIT == state)
            {
                workCount += init();
            }

            if (State.REPLAY == state)
            {
                workCount += replay();
            }
        }
        catch (final IOException ex)
        {
            onError("IOException - " + ex.getMessage() + " - " + segmentFile.getName());
            LangUtil.rethrowUnchecked(ex);
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

    private int init() throws IOException
    {
        if (null == fileChannel)
        {
            final int positionBitsToShift = publication.positionBitsToShift();
            final long startTermBasePosition = startPosition - (startPosition & (termLength - 1));
            final int segmentOffset = (int)(replayPosition - startTermBasePosition) & (segmentLength - 1);
            final int termId = ((int)(replayPosition >> positionBitsToShift) + publication.initialTermId());

            openRecordingSegment();

            termOffset = (int)(replayPosition & (termLength - 1));
            termBaseSegmentOffset = segmentOffset - termOffset;

            if (replayPosition > startPosition && replayPosition != stopPosition)
            {
                if (notHeaderAligned(fileChannel, replayBuffer, segmentOffset, termOffset, termId, streamId))
                {
                    onError(replayPosition + " position not aligned to data header");
                    return 0;
                }
            }
        }

        if (!publication.isConnected())
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

    private int replay() throws IOException
    {
        int fragments = 0;

        if (recordingPosition != null && replayPosition >= stopPosition && noNewData(replayPosition, stopPosition))
        {
            return fragments;
        }

        if (termOffset == termLength)
        {
            nextTerm();
        }

        int frameOffset = 0;
        final int bytesRead = readRecording(stopPosition - replayPosition);

        while (frameOffset < bytesRead)
        {
            final int frameLength = FrameDescriptor.frameLength(replayBuffer, frameOffset);
            final int frameType = FrameDescriptor.frameType(replayBuffer, frameOffset);
            final int alignedLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
            final int dataOffset = frameOffset + DataHeaderFlyweight.HEADER_LENGTH;
            final int dataLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;

            long result = 0;
            if (frameType == HeaderFlyweight.HDR_TYPE_DATA)
            {
                if (frameOffset + alignedLength > bytesRead)
                {
                    break;
                }

                result = publication.tryClaim(dataLength, bufferClaim);
                if (result > 0)
                {
                    bufferClaim
                        .flags(FrameDescriptor.frameFlags(replayBuffer, frameOffset))
                        .reservedValue(replayBuffer.getLong(frameOffset + RESERVED_VALUE_OFFSET, LITTLE_ENDIAN))
                        .putBytes(replayBuffer, dataOffset, dataLength)
                        .commit();
                }
            }
            else if (frameType == HeaderFlyweight.HDR_TYPE_PAD)
            {
                result = publication.appendPadding(dataLength);
            }

            if (result > 0)
            {
                fragments++;
                frameOffset += alignedLength;
                termOffset += alignedLength;
                replayPosition += alignedLength;

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

    private int readRecording(final long availableReplay) throws IOException
    {
        if (publication.availableWindow() > 0)
        {
            final int limit = Math.min((int)Math.min(availableReplay, MAX_BLOCK_LENGTH), termLength - termOffset);
            final ByteBuffer byteBuffer = replayBuffer.byteBuffer();
            byteBuffer.clear().limit(limit);

            int position = termBaseSegmentOffset + termOffset;
            do
            {
                position += fileChannel.read(byteBuffer, position);
            }
            while (byteBuffer.remaining() > 0);

            return byteBuffer.limit();
        }
        else if (!publication.isConnected())
        {
            state = State.INACTIVE;
        }

        return 0;
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

    private void nextTerm() throws IOException
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
    }

    private void closeRecordingSegment()
    {
        CloseHelper.close(fileChannel);
        fileChannel = null;
        segmentFile = null;
    }

    private void openRecordingSegment() throws IOException
    {
        if (null == segmentFile)
        {
            final String segmentFileName = segmentFileName(recordingId, segmentFileIndex);
            segmentFile = new File(archiveDir, segmentFileName);

            if (!segmentFile.exists())
            {
                final String msg = "recording segment not found " + segmentFileName;
                onError(msg);
                throw new ArchiveException(msg);
            }
        }

        fileChannel = FileChannel.open(segmentFile.toPath(), FILE_OPTIONS, NO_ATTRIBUTES);
    }

    static boolean notHeaderAligned(
        final FileChannel channel,
        final UnsafeBuffer buffer,
        final int segmentOffset,
        final int termOffset,
        final int termId,
        final int streamId) throws IOException
    {
        final ByteBuffer byteBuffer = buffer.byteBuffer();
        byteBuffer.clear().limit(DataHeaderFlyweight.HEADER_LENGTH);
        if (DataHeaderFlyweight.HEADER_LENGTH != channel.read(byteBuffer, segmentOffset))
        {
            throw new ArchiveException("failed to read fragment header");
        }

        return isInvalidHeader(buffer, streamId, termId, termOffset);
    }

    static boolean isInvalidHeader(
        final UnsafeBuffer buffer, final int streamId, final int termId, final int termOffset)
    {
        return
            DataHeaderFlyweight.termOffset(buffer, 0) != termOffset ||
            DataHeaderFlyweight.termId(buffer, 0) != termId ||
            DataHeaderFlyweight.streamId(buffer, 0) != streamId;
    }
}

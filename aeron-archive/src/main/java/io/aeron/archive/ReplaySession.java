/*
 * Copyright 2014-2022 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
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
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.lang.Math.min;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardOpenOption.READ;
import static org.agrona.BitUtil.align;

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

    private final long connectDeadlineMs;
    private final long correlationId;
    private final long sessionId;
    private final long recordingId;
    private final long startPosition;
    private long replayPosition;
    private long stopPosition;
    private long replayLimit;
    private volatile long segmentFileBasePosition;
    private int termBaseSegmentOffset;
    private int termOffset;
    private final int streamId;
    private final int termLength;
    private final int segmentLength;

    private final long replayBufferAddress;
    private final Checksum checksum;

    private final ExclusivePublication publication;
    private final ControlSession controlSession;
    private final CachedEpochClock epochClock;
    private final File archiveDir;
    private final Catalog catalog;
    private final Counter limitPosition;
    private final UnsafeBuffer replayBuffer;
    private final int mtuLength;
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
        final CachedEpochClock epochClock,
        final ExclusivePublication publication,
        final RecordingSummary recordingSummary,
        final Counter replayLimitPosition,
        final Checksum checksum)
    {
        this.controlSession = controlSession;
        this.sessionId = replaySessionId;
        this.correlationId = correlationId;
        this.recordingId = recordingSummary.recordingId;
        this.segmentLength = recordingSummary.segmentFileLength;
        this.termLength = recordingSummary.termBufferLength;
        this.streamId = recordingSummary.streamId;
        this.mtuLength = recordingSummary.mtuLength;
        this.epochClock = epochClock;
        this.archiveDir = archiveDir;
        this.publication = publication;
        this.limitPosition = replayLimitPosition;
        this.replayBuffer = replayBuffer;
        this.replayBufferAddress = replayBuffer.addressOffset();
        this.catalog = catalog;
        this.checksum = checksum;
        this.startPosition = recordingSummary.startPosition;
        this.stopPosition = null == limitPosition ? recordingSummary.stopPosition : limitPosition.get();

        final long fromPosition = position == NULL_POSITION ? startPosition : position;
        final long maxLength = null == limitPosition ? stopPosition - fromPosition : Long.MAX_VALUE - fromPosition;
        final long replayLength = length == AeronArchive.NULL_LENGTH ? maxLength : min(length, maxLength);
        if (replayLength < 0)
        {
            close();
            final String msg = "replay recording " + recordingId + " - " + "length must be positive: " + replayLength;
            controlSession.attemptErrorResponse(correlationId, msg, controlResponseProxy);
            throw new ArchiveException(msg);
        }

        if (null != limitPosition)
        {
            final long currentPosition = limitPosition.get();
            if (currentPosition < fromPosition)
            {
                close();
                final String msg = "replay recording " + recordingId + " - " +
                    fromPosition + " after current position of " + currentPosition;
                controlSession.attemptErrorResponse(correlationId, msg, controlResponseProxy);
                throw new ArchiveException(msg);
            }
        }

        segmentFileBasePosition = AeronArchive.segmentFileBasePosition(
            startPosition, fromPosition, termLength, segmentLength);
        replayPosition = fromPosition;
        replayLimit = fromPosition + replayLength;

        final String segmentFileName = segmentFileName(recordingId, segmentFileBasePosition);
        segmentFile = new File(archiveDir, segmentFileName);

        controlSession.sendOkResponse(correlationId, replaySessionId, controlResponseProxy);
        connectDeadlineMs = epochClock.time() + connectTimeoutMs;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        final CountedErrorHandler errorHandler = controlSession.archiveConductor().context().countedErrorHandler();
        CloseHelper.close(errorHandler, publication);
        CloseHelper.close(errorHandler, fileChannel);
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return sessionId;
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = 0;

        if (isAborted)
        {
            state(State.INACTIVE);
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
            state(State.DONE);
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public void abort()
    {
        isAborted = true;
    }

    /**
     * {@inheritDoc}
     */
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

    String replayChannel()
    {
        return publication.channel();
    }

    int replayStreamId()
    {
        return publication.streamId();
    }

    long segmentFileBasePosition()
    {
        return segmentFileBasePosition;
    }

    Counter limitPosition()
    {
        return limitPosition;
    }

    void sendPendingError(final ControlResponseProxy controlResponseProxy)
    {
        if (null != errorMessage && !controlSession.isDone())
        {
            onPendingError(sessionId, recordingId, errorMessage);
            controlSession.attemptErrorResponse(correlationId, errorMessage, controlResponseProxy);
        }
    }

    @SuppressWarnings("unused")
    void onPendingError(final long sessionId, final long recordingId, final String errorMessage)
    {
        // Hook for Agent logging
    }

    private int init() throws IOException
    {
        if (replayBuffer.capacity() < mtuLength)
        {
            onError("fileIoMaxLength is less than mtuLength");
            return 0;
        }

        if (null == fileChannel)
        {
            if (!segmentFile.exists())
            {
                if (epochClock.time() > connectDeadlineMs)
                {
                    onError("recording segment not found " + segmentFile);
                }

                return 0;
            }
            else
            {
                final long startTermBasePosition = startPosition - (startPosition & (termLength - 1));
                final int segmentOffset = (int)((replayPosition - startTermBasePosition) & (segmentLength - 1));
                final int termId = LogBufferDescriptor.computeTermIdFromPosition(
                    replayPosition, publication.positionBitsToShift(), publication.initialTermId());

                openRecordingSegment();

                termOffset = (int)(replayPosition & (termLength - 1));
                termBaseSegmentOffset = segmentOffset - termOffset;

                if (replayPosition > startPosition && replayPosition != stopPosition)
                {
                    if (notHeaderAligned(fileChannel, replayBuffer, segmentOffset, termOffset, termId, streamId))
                    {
                        onError(replayPosition + " position not aligned to a data header");
                        return 0;
                    }
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

        state(State.REPLAY);

        return 1;
    }

    private int replay() throws IOException
    {
        if (!publication.isConnected())
        {
            state(State.INACTIVE);
            return 0;
        }

        if (replayPosition >= stopPosition && null != limitPosition && noNewData(replayPosition, stopPosition))
        {
            return 0;
        }

        if (termOffset == termLength)
        {
            nextTerm();
        }

        int workCount = 0;
        final int bytesRead = readRecording(stopPosition - replayPosition);
        if (bytesRead > 0)
        {
            int batchOffset = 0;
            int paddingFrameLength = 0;
            final int sessionId = publication.sessionId();
            final int streamId = publication.streamId();
            final long remaining = replayLimit - replayPosition;
            final Checksum checksum = this.checksum;
            final UnsafeBuffer replayBuffer = this.replayBuffer;

            while (batchOffset < bytesRead && batchOffset < remaining)
            {
                final int frameLength = frameLength(replayBuffer, batchOffset);
                if (frameLength <= 0)
                {
                    raiseError(frameLength, bytesRead, batchOffset, remaining);
                }

                final int frameType = frameType(replayBuffer, batchOffset);
                final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

                if (HDR_TYPE_DATA == frameType)
                {
                    if (batchOffset + alignedLength > bytesRead)
                    {
                        break;
                    }

                    if (null != checksum)
                    {
                        verifyChecksum(checksum, batchOffset, alignedLength);
                    }

                    replayBuffer.putInt(batchOffset + SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);
                    replayBuffer.putInt(batchOffset + STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);
                    batchOffset += alignedLength;
                }
                else if (HDR_TYPE_PAD == frameType)
                {
                    paddingFrameLength = frameLength;
                    break;
                }
            }

            if (batchOffset > 0)
            {
                final long position = publication.offerBlock(replayBuffer, 0, batchOffset);
                if (hasPublicationAdvanced(position, batchOffset))
                {
                    workCount++;
                }
                else
                {
                    paddingFrameLength = 0;
                }
            }

            if (paddingFrameLength > 0)
            {
                final long position = publication.appendPadding(paddingFrameLength - HEADER_LENGTH);
                if (hasPublicationAdvanced(position, align(paddingFrameLength, FRAME_ALIGNMENT)))
                {
                    workCount++;
                }
            }
        }

        return workCount;
    }

    private void raiseError(final int frameLength, final int bytesRead, final int batchOffset, final long remaining)
    {
        throw new IllegalStateException("unexpected end of recording " + recordingId +
            " frameLength=" + frameLength +
            " replayPosition=" + replayPosition +
            " remaining=" + remaining +
            " limitPosition=" + limitPosition +
            " batchOffset=" + batchOffset +
            " bytesRead=" + bytesRead);
    }

    private boolean hasPublicationAdvanced(final long position, final int alignedLength)
    {
        if (position > 0)
        {
            termOffset += alignedLength;
            replayPosition += alignedLength;

            if (replayPosition >= replayLimit)
            {
                state(State.INACTIVE);
            }

            return true;
        }
        else if (Publication.CLOSED == position || Publication.NOT_CONNECTED == position)
        {
            onError("stream closed before replay complete");
        }

        return false;
    }

    private void verifyChecksum(final Checksum checksum, final int frameOffset, final int alignedLength)
    {
        final int computedChecksum = checksum.compute(
            replayBufferAddress, frameOffset + HEADER_LENGTH, alignedLength - HEADER_LENGTH);
        final int recordedChecksum = frameSessionId(replayBuffer, frameOffset);

        if (computedChecksum != recordedChecksum)
        {
            final String message = "CRC checksum mismatch at offset=" + frameOffset + ": recorded checksum=" +
                recordedChecksum + ", computed checksum=" + computedChecksum;
            throw new ArchiveException(message);
        }
    }

    private int readRecording(final long availableReplay) throws IOException
    {
        if (publication.availableWindow() > 0)
        {
            final int limit = min((int)min(availableReplay, replayBuffer.capacity()), termLength - termOffset);
            final ByteBuffer byteBuffer = replayBuffer.byteBuffer();
            byteBuffer.clear().limit(limit);

            int position = termBaseSegmentOffset + termOffset;
            do
            {
                final int bytesRead = fileChannel.read(byteBuffer, position);
                if (bytesRead <= 0)
                {
                    break;
                }

                position += bytesRead;
            }
            while (byteBuffer.remaining() > 0);

            return byteBuffer.limit();
        }

        return 0;
    }

    private void onError(final String errorMessage)
    {
        this.errorMessage = errorMessage;
        state(State.INACTIVE);
    }

    private boolean noNewData(final long replayPosition, final long oldStopPosition)
    {
        final Counter limitPosition = this.limitPosition;
        final long currentLimitPosition = limitPosition.get();
        final boolean isCounterClosed = limitPosition.isClosed();
        final long newStopPosition = isCounterClosed ? catalog.stopPosition(recordingId) : currentLimitPosition;

        if (isCounterClosed)
        {
            if (NULL_POSITION == newStopPosition)
            {
                replayLimit = oldStopPosition;
            }
            else if (newStopPosition < replayLimit)
            {
                replayLimit = newStopPosition;
            }
        }

        if (replayPosition >= replayLimit)
        {
            state(State.INACTIVE);
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
            segmentFileBasePosition += segmentLength;
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
            final String segmentFileName = segmentFileName(recordingId, segmentFileBasePosition);
            segmentFile = new File(archiveDir, segmentFileName);

            if (!segmentFile.exists())
            {
                final String msg = "recording segment not found " + segmentFileName;
                onError(msg);
                throw new ArchiveException(msg);
            }
        }

        fileChannel = FileChannel.open(segmentFile.toPath(), FILE_OPTIONS);
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
        byteBuffer.clear().limit(HEADER_LENGTH);
        if (HEADER_LENGTH != channel.read(byteBuffer, segmentOffset))
        {
            throw new ArchiveException("failed to read fragment header");
        }

        return isInvalidHeader(buffer, streamId, termId, termOffset);
    }

    private void state(final State newState)
    {
        //System.out.println("ReplaySession: " + epochClock.time() + ": " + state + " -> " + newState);
        state = newState;
    }

    static boolean isInvalidHeader(
        final UnsafeBuffer buffer, final int streamId, final int termId, final int termOffset)
    {
        return
            termOffset(buffer, 0) != termOffset ||
            termId(buffer, 0) != termId ||
            streamId(buffer, 0) != streamId;
    }
}

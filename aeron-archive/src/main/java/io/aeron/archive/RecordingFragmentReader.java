/*
 * Copyright 2014-2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive;

import io.aeron.Counter;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
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
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.RESERVED_VALUE_OFFSET;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.*;

class RecordingFragmentReader implements AutoCloseable
{
    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(READ);
    private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute[0];

    private final File archiveDir;
    private final long recordingId;
    private final int segmentLength;
    private final int termLength;

    private final Catalog catalog;
    private final Counter recordingPosition;
    private final UnsafeBuffer termBuffer;
    private MappedByteBuffer mappedSegmentBuffer;

    private long stopPosition;
    private long replayPosition;
    private long replayLimit;
    private int termOffset;
    private int termBaseSegmentOffset;
    private int segmentFileIndex;
    private boolean isDone = false;

    RecordingFragmentReader(
        final Catalog catalog,
        final RecordingSummary recordingSummary,
        final File archiveDir,
        final long position,
        final long length,
        final Counter recordingPosition)
    {
        this.catalog = catalog;
        this.archiveDir = archiveDir;
        this.recordingPosition = recordingPosition;
        this.termLength = recordingSummary.termBufferLength;
        this.segmentLength = recordingSummary.segmentFileLength;
        this.recordingId = recordingSummary.recordingId;

        final long startPosition = recordingSummary.startPosition;
        final long fromPosition = position == NULL_POSITION ? startPosition : position;
        final long stopPosition = recordingSummary.stopPosition;
        this.stopPosition = stopPosition == NULL_POSITION ? recordingPosition.get() : stopPosition;

        final long maxLength = recordingPosition == null ? stopPosition - fromPosition : Long.MAX_VALUE - fromPosition;
        final long replayLength = length == AeronArchive.NULL_LENGTH ? maxLength : Math.min(length, maxLength);
        if (replayLength < 0)
        {
            throw new IllegalArgumentException("length must be positive");
        }

        segmentFileIndex = segmentFileIndex(startPosition, fromPosition, segmentLength);
        openRecordingSegment();

        final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
        final long startTermBasePosition = startPosition - (startPosition & (termLength - 1));
        final int segmentOffset = (int)(fromPosition - startTermBasePosition) & (segmentLength - 1);
        final int termId = ((int)(fromPosition >> positionBitsToShift) + recordingSummary.initialTermId);

        termOffset = (int)(fromPosition & (termLength - 1));
        termBaseSegmentOffset = segmentOffset - termOffset;
        termBuffer = new UnsafeBuffer(mappedSegmentBuffer, termBaseSegmentOffset, termLength);

        if (fromPosition > startPosition &&
            (DataHeaderFlyweight.termOffset(termBuffer, termOffset) != termOffset ||
            DataHeaderFlyweight.termId(termBuffer, termOffset) != termId ||
            DataHeaderFlyweight.streamId(termBuffer, termOffset) != recordingSummary.streamId))
        {
            close();
            throw new IllegalArgumentException(fromPosition + " position not aligned to valid fragment");
        }

        replayPosition = fromPosition;
        replayLimit = fromPosition + replayLength;
    }

    public void close()
    {
        closeRecordingSegment();
    }

    long recordingId()
    {
        return recordingId;
    }

    boolean isDone()
    {
        return isDone;
    }

    int controlledPoll(final SimpleFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        int fragments = 0;

        if (noAvailableLiveData())
        {
            return fragments;
        }

        final UnsafeBuffer termBuffer = this.termBuffer;
        while (replayPosition < stopPosition && fragments < fragmentLimit)
        {
            final int frameOffset = termOffset;
            if (frameOffset == termLength)
            {
                termOffset = 0;
                nextTerm();
                break;
            }

            final int frameLength = FrameDescriptor.frameLength(termBuffer, frameOffset);
            final int frameType = FrameDescriptor.frameType(termBuffer, frameOffset);
            final byte flags = FrameDescriptor.frameFlags(termBuffer, frameOffset);
            final long reservedValue = termBuffer.getLong(frameOffset + RESERVED_VALUE_OFFSET, LITTLE_ENDIAN);

            final int alignedLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
            final int dataOffset = frameOffset + DataHeaderFlyweight.HEADER_LENGTH;
            final int dataLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;

            if (!fragmentHandler.onFragment(termBuffer, dataOffset, dataLength, frameType, flags, reservedValue))
            {
                break;
            }

            replayPosition += alignedLength;
            termOffset += alignedLength;
            fragments++;

            if (replayPosition >= replayLimit)
            {
                isDone = true;
                closeRecordingSegment();
                break;
            }
        }

        return fragments;
    }

    static boolean hasInitialSegmentFile(
        final RecordingSummary recordingSummary, final File archiveDir, final long position)
    {
        final long fromPosition = position == NULL_POSITION ? recordingSummary.startPosition : position;
        final int segmentFileIndex = segmentFileIndex(
            recordingSummary.startPosition, fromPosition, recordingSummary.segmentFileLength);
        final File segmentFile = new File(archiveDir, segmentFileName(recordingSummary.recordingId, segmentFileIndex));

        return segmentFile.exists();
    }

    private boolean noAvailableLiveData()
    {
        return recordingPosition != null &&
            replayPosition == stopPosition &&
            !refreshStopPositionAndLimit(replayPosition, stopPosition);
    }

    private boolean refreshStopPositionAndLimit(final long replayPosition, final long oldStopPosition)
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
            isDone = true;
            return false;
        }

        if (newStopPosition != oldStopPosition)
        {
            stopPosition = newStopPosition;
            return true;
        }

        return false;
    }

    private void nextTerm()
    {
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
}

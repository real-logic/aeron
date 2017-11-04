/*
 * Copyright 2014-2017 Real Logic Ltd.
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

import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.IoUtil;
import org.agrona.UnsafeAccess;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archive.Archive.segmentFileIndex;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;

class RecordingFragmentReader implements AutoCloseable
{
    static final long NULL_POSITION = -1;
    static final long NULL_LENGTH = -1;

    private final File archiveDir;
    private final long recordingId;
    private final long startPosition;
    private final int segmentLength;
    private final int termLength;

    private final Catalog catalog;
    private final AtomicCounter recordingPosition;
    private final UnsafeBuffer termBuffer;
    private MappedByteBuffer mappedSegmentBuffer;

    private long fromPosition;
    private long stopPosition;
    private long replayPosition;
    private long replayLimit;
    private int termOffset;
    private int termStartSegmentOffset;
    private int segmentFileIndex;
    private boolean isDone = false;

    RecordingFragmentReader(
        final Catalog catalog,
        final RecordingSummary recordingSummary,
        final File archiveDir,
        final long position,
        final long length,
        final AtomicCounter recordingPosition) throws IOException
    {
        this.catalog = catalog;
        stopPosition = recordingSummary.stopPosition;
        termLength = recordingSummary.termBufferLength;
        segmentLength = recordingSummary.segmentFileLength;
        startPosition = recordingSummary.startPosition;
        recordingId = recordingSummary.recordingId;

        if (stopPosition == NULL_POSITION)
        {
            if (recordingPosition == null)
            {
                throw new IllegalArgumentException(
                    "Recording descriptor indicates live recording, but recordedPosition" +
                        " is null. Replay for recording id:" + recordingId);
            }
            else if (recordingPosition.isClosed())
            {
                // could re-read, to handle race of closing stream while constructing reader, but this is an
                // unexpected race
                throw new IllegalStateException("Position closed concurrently to replay construction." +
                    " Replay for recording id:" + recordingId);
            }
            else
            {
                stopPosition = recordingPosition.get();
            }
        }

        this.archiveDir = archiveDir;

        fromPosition = position == NULL_POSITION ? startPosition : position;
        if (fromPosition < 0)
        {
            throw new IllegalArgumentException("fromPosition must be positive");
        }

        this.recordingPosition = recordingPosition;
        final long maxLength = recordingPosition == null ? stopPosition - fromPosition : Long.MAX_VALUE - fromPosition;
        final long replayLength = length == NULL_LENGTH ? maxLength : Math.min(length, maxLength);

        if (replayLength < 0)
        {
            throw new IllegalArgumentException("Length must be positive");
        }

        segmentFileIndex = segmentFileIndex(startPosition, fromPosition, segmentLength);

        if (!openRecordingSegment())
        {
            throw new IllegalStateException("segment file must be available for requested position: " + position);
        }

        final long termStartPosition = (startPosition / termLength) * termLength;
        final long fromSegmentOffset = (fromPosition - termStartPosition) & (segmentLength - 1);
        final int termMask = termLength - 1;
        final int fromTermStartSegmentOffset = (int)(fromSegmentOffset - (fromSegmentOffset & termMask));
        final int fromTermOffset = (int)(fromSegmentOffset & termMask);

        termBuffer = new UnsafeBuffer(mappedSegmentBuffer, fromTermStartSegmentOffset, termLength);
        termStartSegmentOffset = fromTermStartSegmentOffset;
        termOffset = fromTermOffset;

        if (DataHeaderFlyweight.termOffset(termBuffer, fromTermOffset) != fromTermOffset ||
            DataHeaderFlyweight.sessionId(termBuffer, fromTermOffset) != recordingSummary.sessionId ||
            DataHeaderFlyweight.streamId(termBuffer, fromTermOffset) != recordingSummary.streamId)
        {
            close();
            throw new IllegalArgumentException("fromPosition is not aligned to fragment: " + fromPosition);
        }

        replayPosition = fromPosition;

        // from above restrictions on arguments replay limit is in the range [replayPosition, MAX_LONG]
        replayLimit = fromPosition + replayLength;
    }

    public void close()
    {
        closeRecordingSegment();
    }

    boolean isDone()
    {
        return isDone;
    }

    long fromPosition()
    {
        return fromPosition;
    }

    int controlledPoll(final SimplifiedControlledFragmentHandler fragmentHandler, final int fragmentLimit)
        throws IOException
    {
        if (isDone() || noAvailableData())
        {
            return 0;
        }

        int polled = 0;

        while ((stopPosition - replayPosition) > 0 && polled < fragmentLimit)
        {
            if (termOffset == termLength)
            {
                termOffset = 0;
                nextTerm();
                break;
            }

            final int frameOffset = termOffset;
            final int frameLength = FrameDescriptor.frameLength(termBuffer, frameOffset);
            final int alignedLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);

            replayPosition += alignedLength;
            termOffset += alignedLength;

            final int dataOffset = frameOffset + DataHeaderFlyweight.DATA_OFFSET;
            final int dataLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;

            if (!fragmentHandler.onFragment(termBuffer, dataOffset, dataLength))
            {
                replayPosition -= alignedLength;
                termOffset -= alignedLength;
                break;
            }

            polled++;

            if (replayLimit <= replayPosition)
            {
                isDone = true;
                closeRecordingSegment();
                break;
            }
        }

        return polled;
    }

    private boolean noAvailableData()
    {
        return recordingPosition != null &&
            replayPosition == stopPosition &&
            !refreshStopPositionAndLimit(replayPosition, stopPosition);
    }

    private boolean refreshStopPositionAndLimit(final long replayPosition, final long oldStopPosition)
    {
        final long currentRecodingPosition = recordingPosition.get();
        UnsafeAccess.UNSAFE.loadFence();
        final boolean hasRecordingStopped = recordingPosition.isClosed();
        final long newStopPosition = hasRecordingStopped ? catalog.stopPosition(recordingId) : currentRecodingPosition;

        if (hasRecordingStopped && newStopPosition < replayLimit)
        {
            replayLimit = newStopPosition;
        }

        if (replayLimit <= replayPosition)
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

    private void nextTerm() throws IOException
    {
        termStartSegmentOffset += termLength;

        if (termStartSegmentOffset == segmentLength)
        {
            closeRecordingSegment();
            segmentFileIndex++;
            if (!openRecordingSegment())
            {
                throw new IllegalStateException("Failed to open segment file: " +
                    segmentFileName(recordingId, segmentFileIndex));
            }

            termStartSegmentOffset = 0;
        }

        termBuffer.wrap(mappedSegmentBuffer, termStartSegmentOffset, termLength);
    }

    private void closeRecordingSegment()
    {
        if (null != mappedSegmentBuffer)
        {
            IoUtil.unmap(mappedSegmentBuffer);
        }

        mappedSegmentBuffer = null;
    }

    private boolean openRecordingSegment() throws IOException
    {
        final String segmentFileName = segmentFileName(recordingId, segmentFileIndex);
        final File segmentFile = new File(archiveDir, segmentFileName);

        if (!segmentFile.exists())
        {
            final int lastSegmentIndex = segmentFileIndex(startPosition, stopPosition, segmentLength);
            if (lastSegmentIndex > segmentFileIndex)
            {
                throw new IllegalStateException("Recording segment not found. Segment index=" + segmentFileIndex +
                    ", last segment index=" + lastSegmentIndex);
            }

            return false;
        }

        try (FileChannel channel = FileChannel.open(segmentFile.toPath(), READ))
        {
            mappedSegmentBuffer = channel.map(READ_ONLY, 0, segmentLength);
        }

        return true;
    }
}

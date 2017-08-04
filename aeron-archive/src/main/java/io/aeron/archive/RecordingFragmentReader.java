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

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
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

import static io.aeron.archive.ArchiveUtil.segmentFileName;
import static io.aeron.archive.ArchiveUtil.segmentFileIndex;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;

class RecordingFragmentReader implements AutoCloseable
{
    interface SimplifiedControlledPoll
    {
        /**
         * Called by the {@link RecordingFragmentReader}. Implementors need to process DATA and PADDING fragments.
         *
         * @return true if fragment processed, false to abort.
         */
        boolean onFragment(UnsafeBuffer fragmentBuffer, int fragmentOffset, int fragmentLength);
    }

    static final long NULL_POSITION = -1;
    static final long NULL_LENGTH = -1;

    private final File archiveDir;
    private final long recordingId;
    private final long startPosition;
    private final int segmentFileLength;
    private final int termBufferLength;

    private long fromPosition;
    private final AtomicCounter recordingPosition;
    private final RecordingDescriptorDecoder descriptorDecoder;
    private MappedByteBuffer mappedSegmentBuffer;
    private UnsafeBuffer termBuffer = null;

    private int segmentFileIndex;
    private int termStartSegmentOffset;
    private int termOffset;

    private long replayPosition;
    private long replayLimit;
    private long stopPosition;
    private boolean isDone = false;

    RecordingFragmentReader(
        final RecordingDescriptorDecoder descriptorDecoder,
        final File archiveDir,
        final long position,
        final long length,
        final AtomicCounter recordingPosition) throws IOException
    {
        this.descriptorDecoder = descriptorDecoder;
        stopPosition = descriptorDecoder.stopPosition();
        termBufferLength = descriptorDecoder.termBufferLength();
        segmentFileLength = descriptorDecoder.segmentFileLength();
        startPosition = descriptorDecoder.startPosition();

        recordingId = descriptorDecoder.recordingId();
        this.archiveDir = archiveDir;

        fromPosition = position == NULL_POSITION ? startPosition : position;
        this.recordingPosition = recordingPosition;
        final long maxLength = recordingPosition == null ? stopPosition - fromPosition : Long.MAX_VALUE;
        final long replayLength = length == NULL_LENGTH ? maxLength : Math.min(length, maxLength);

        segmentFileIndex = segmentFileIndex(startPosition, fromPosition, segmentFileLength);

        if (!openRecordingSegment())
        {
            throw new IllegalStateException("First file must be available");
        }

        final long termStartPosition = (startPosition / termBufferLength) * termBufferLength;
        final long fromSegmentOffset = (fromPosition - termStartPosition) & (segmentFileLength - 1);
        final int termMask = termBufferLength - 1;
        final int fromTermStartSegmentOffset = (int)(fromSegmentOffset - (fromSegmentOffset & termMask));
        final int fromTermOffset = (int)(fromSegmentOffset & termMask);

        termBuffer = new UnsafeBuffer(mappedSegmentBuffer, fromTermStartSegmentOffset, termBufferLength);
        termStartSegmentOffset = fromTermStartSegmentOffset;
        termOffset = fromTermOffset;
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(termBuffer, termOffset, DataHeaderFlyweight.HEADER_LENGTH);

        if (flyweight.sessionId() != descriptorDecoder.sessionId() ||
            flyweight.streamId() != descriptorDecoder.streamId() ||
            flyweight.termOffset() != termOffset)
        {
            close();
            throw new IllegalArgumentException("fromPosition:" + fromPosition + " is not aligned to fragment");
        }

        replayPosition = fromPosition;
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

    int controlledPoll(final SimplifiedControlledPoll fragmentHandler, final int fragmentLimit)
        throws IOException
    {
        if (isDone() || noAvailableData())
        {
            return 0;
        }

        int polled = 0;

        while ((stopPosition - replayPosition) > 0 && polled < fragmentLimit)
        {
            if (termOffset == termBufferLength)
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
            // TODO: if length crosses a fragment boundary we will send more than requested, consider under supplying
            if ((replayLimit - replayPosition) <= 0)
            {
                isDone = true;
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
        final long stopTimestamp = currentRecordingStopTimestamp();
        final long recordingPosition = this.recordingPosition.get();
        final long newStopPosition = this.recordingPosition.isClosed() ? descriptorStopPosition() : recordingPosition;

        if (stopTimestamp != Catalog.NULL_TIME && (newStopPosition - replayLimit) < 0)
        {
            replayLimit = newStopPosition;
        }

        if ((replayLimit - replayPosition) <= 0)
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

    private long currentRecordingStopTimestamp()
    {
        UnsafeAccess.UNSAFE.loadFence();
        return descriptorDecoder.stopTimestamp();
    }

    private long descriptorStopPosition()
    {
        UnsafeAccess.UNSAFE.loadFence();
        return descriptorDecoder.stopPosition();
    }

    private void nextTerm() throws IOException
    {
        termStartSegmentOffset += termBufferLength;

        if (termStartSegmentOffset == segmentFileLength)
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

        termBuffer.wrap(mappedSegmentBuffer, termStartSegmentOffset, termBufferLength);
    }

    private void closeRecordingSegment()
    {
        IoUtil.unmap(mappedSegmentBuffer);
    }

    private boolean openRecordingSegment() throws IOException
    {
        final String segmentFileName = segmentFileName(recordingId, segmentFileIndex);
        final File segmentFile = new File(archiveDir, segmentFileName);
        final long stopPosition = descriptorStopPosition();

        if (!segmentFile.exists())
        {
            final int lastSegmentIndex = segmentFileIndex(startPosition, stopPosition, segmentFileLength);
            if (lastSegmentIndex > segmentFileIndex)
            {
                throw new IllegalStateException("Recording segment not found. Segment index=" + segmentFileIndex +
                    ", last segment index=" + lastSegmentIndex);
            }

            return false;
        }

        try (FileChannel channel = FileChannel.open(segmentFile.toPath(), READ))
        {
            mappedSegmentBuffer = channel.map(READ_ONLY, 0, segmentFileLength);
        }

        return true;
    }
}

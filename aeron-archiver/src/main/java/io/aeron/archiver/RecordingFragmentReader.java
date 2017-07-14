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
package io.aeron.archiver;

import io.aeron.archiver.codecs.RecordingDescriptorDecoder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.IoUtil;
import org.agrona.UnsafeAccess;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveUtil.recordingDataFileName;
import static io.aeron.archiver.ArchiveUtil.segmentFileIndex;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;

class RecordingFragmentReader implements AutoCloseable
{

    interface SimplifiedControlledPoll
    {
        /**
         * Called by the {@link RecordingFragmentReader}. Implementors need to process DATA and PAD fragments.
         *
         * @return true if fragment processed, false to abort.
         */
        boolean onFragment(UnsafeBuffer fragmentBuffer, int fragmentOffset, int fragmentLength);
    }

    static final long NULL_POSITION = -1;
    static final long NULL_LENGTH = -1;

    private final long recordingId;
    private final File archiveDir;
    private final int termBufferLength;
    private final int segmentFileLength;
    private final long fromPosition;
    private final long replayLength;
    private final RecordingDescriptorDecoder descriptorDecoder;

    private MappedByteBuffer mappedSegmentBuffer;

    private UnsafeBuffer termBuffer = null;

    private int segmentFileIndex;
    private int termStartSegmentOffset;
    private int termOffset;

    private long replayPosition;
    private long replayLimit;
    private long endPosition;
    private boolean isDone = false;

    RecordingFragmentReader(
        final RecordingDescriptorDecoder descriptorDecoder,
        final File archiveDir,
        final long position,
        final long length) throws IOException
    {
        this.descriptorDecoder = descriptorDecoder;
        this.endPosition = descriptorDecoder.endPosition();
        this.termBufferLength = descriptorDecoder.termBufferLength();
        this.segmentFileLength = descriptorDecoder.segmentFileLength();
        final long joinPosition = descriptorDecoder.joinPosition();
        final long recordingLength = endPosition - joinPosition;

        this.recordingId = descriptorDecoder.recordingId();
        this.archiveDir = archiveDir;

        final long replayLength = length == NULL_LENGTH ? recordingLength : length;
        final long fromPosition = position == NULL_POSITION ? joinPosition : position;

        segmentFileIndex = segmentFileIndex(joinPosition, fromPosition, segmentFileLength);
        final long joinTermStartPosition = (joinPosition / termBufferLength) * termBufferLength;

        openRecordingFile();

        final long fromSegmentOffset = (fromPosition - joinTermStartPosition) & (segmentFileLength - 1);
        final int fromTermStartSegmentOffset = (int)(fromSegmentOffset - (fromSegmentOffset & (termBufferLength - 1)));
        final int fromTermOffset = (int)(fromSegmentOffset & (termBufferLength - 1));

        termBuffer = new UnsafeBuffer(mappedSegmentBuffer, fromTermStartSegmentOffset, termBufferLength);
        termStartSegmentOffset = fromTermStartSegmentOffset;
        termOffset = fromTermOffset;

        final int fragmentOffset;
        if (segmentFileIndex == 0 && fromTermStartSegmentOffset == 0)
        {
            fragmentOffset = findFromFragmentOffset((int)(joinPosition & (termBufferLength - 1)));
        }
        else
        {
            fragmentOffset = findFromFragmentOffset(0);
        }

        if (fragmentOffset != termOffset)
        {
            final int alignmentOffset = fragmentOffset - termOffset;
            this.fromPosition = fromPosition + alignmentOffset;
            this.replayLength = replayLength - alignmentOffset;
        }
        else
        {
            this.fromPosition = fromPosition;
            this.replayLength = replayLength;
        }
        replayPosition = this.fromPosition;
        replayLimit = this.fromPosition + this.replayLength;
        if (fragmentOffset >= termBufferLength)
        {
            termOffset = 0;
            nextTerm();
        }
        else
        {
            termOffset = fragmentOffset;
        }
    }

    public void close()
    {
        closeRecordingFile();
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
        if (isDone())
        {
            return 0;
        }

        final long oldEndPosition = this.endPosition;
        if (replayPosition == oldEndPosition)
        {
            refreshEndPositionAndLimit(replayPosition, oldEndPosition);
        }
        int polled = 0;

        while ((endPosition - replayPosition) > 0 && polled < fragmentLimit)
        {
            final int frameOffset = termOffset;
            final int frameLength = FrameDescriptor.frameLength(termBuffer, frameOffset);

            if (frameLength == 0)
            {
                break;
            }

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
            if ((replayLimit - replayPosition) <= 0)
            {
                isDone = true;
                break;
            }
            else if (termOffset == termBufferLength)
            {
                termOffset = 0;
                nextTerm();
                break;
            }
        }

        return polled;
    }

    private void refreshEndPositionAndLimit(final long replayPosition, final long oldEndPosition)
    {
        final long endTimestamp = currentRecordingEndTimestamp();
        final long newEndPosition = currentRecordingEndPosition();

        if (endTimestamp != Catalog.NULL_TIME && (newEndPosition - this.replayLimit) < 0)
        {
            this.replayLimit = newEndPosition;
        }

        if (newEndPosition != oldEndPosition)
        {
            this.endPosition = newEndPosition;
        }

        if ((replayLimit - replayPosition) <= 0)
        {
            isDone = true;
        }
    }

    private long currentRecordingEndTimestamp()
    {
        UnsafeAccess.UNSAFE.loadFence();
        return descriptorDecoder.endTimestamp();
    }

    private long currentRecordingEndPosition()
    {
        UnsafeAccess.UNSAFE.loadFence();
        return descriptorDecoder.endPosition();
    }

    private void nextTerm() throws IOException
    {
        termStartSegmentOffset += termBufferLength;

        if (termStartSegmentOffset == segmentFileLength)
        {
            closeRecordingFile();
            segmentFileIndex++;
            openRecordingFile();
        }

        termBuffer.wrap(mappedSegmentBuffer, termStartSegmentOffset, termBufferLength);
    }

    private void closeRecordingFile()
    {
        IoUtil.unmap(mappedSegmentBuffer);
    }

    private void openRecordingFile() throws IOException
    {
        termStartSegmentOffset = 0;
        final String recordingDataFileName = recordingDataFileName(recordingId, segmentFileIndex);
        final File recordingDataFile = new File(archiveDir, recordingDataFileName);

        try (FileChannel fileChannel = FileChannel.open(recordingDataFile.toPath(), READ))
        {
            mappedSegmentBuffer = fileChannel.map(READ_ONLY, 0, segmentFileLength);
        }
    }

    /**
     * @param joinTermOffset from which to start searching.
     * @return the offset within the current term, rounded up to the nearest fragment offset
     */
    private int findFromFragmentOffset(final int joinTermOffset)
    {
        int frameOffset = joinTermOffset;

        while (frameOffset < termOffset)
        {
            final int frameLength = FrameDescriptor.frameLength(termBuffer, frameOffset);
            final int alignedLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
            frameOffset += alignedLength;
        }

        return frameOffset;
    }
}

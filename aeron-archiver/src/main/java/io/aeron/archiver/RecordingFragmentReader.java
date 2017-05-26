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
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveUtil.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.PADDING_FRAME_TYPE;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;

class RecordingFragmentReader implements AutoCloseable
{
    interface SimplifiedControlledPoll
    {
        /**
         * Called by the {@link RecordingFragmentReader}. Implementors need only process DATA fragments.
         *
         * @return true if fragment processed, false to abort.
         */
        boolean onFragment(
            DirectBuffer fragmentBuffer,
            int fragmentOffset,
            int fragmentLength,
            DataHeaderFlyweight header);
    }

    private static final long NULL_POSITION = -1;
    private static final long NULL_LENGTH = -1;

    private final long recordingId;
    private final File archiveDir;
    private final int termBufferLength;
    private final long replayLength;
    private final int segmentFileLength;
    private final long fromPosition;

    private int segmentFileIndex;
    private FileChannel currentDataChannel = null;
    private UnsafeBuffer termBuffer = null;
    private int recordingTermStartOffset;
    private int termOffset;
    private long transmitted = 0;
    private final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
    private MappedByteBuffer mappedByteBuffer;

    RecordingFragmentReader(final long recordingId, final File archiveDir) throws IOException
    {
        this(getDescriptor(recordingId, archiveDir), recordingId, archiveDir, NULL_POSITION, NULL_LENGTH);
    }

    RecordingFragmentReader(
        final long recordingId,
        final File archiveDir,
        final long position,
        final long length) throws IOException
    {
        this(getDescriptor(recordingId, archiveDir), recordingId, archiveDir, position, length);
    }

    public void close()
    {
        closeRecordingFile();
    }

    private RecordingFragmentReader(
        final RecordingDescriptorDecoder metaDecoder,
        final long recordingId,
        final File archiveDir,
        final long position,
        final long length) throws IOException
    {
        this.recordingId = recordingId;
        this.archiveDir = archiveDir;
        termBufferLength = metaDecoder.termBufferLength();
        segmentFileLength = metaDecoder.segmentFileLength();
        final long fullLength = ArchiveUtil.recordingFileFullLength(metaDecoder);
        final long joiningPosition = metaDecoder.joiningPosition();

        this.replayLength = length == NULL_LENGTH ? fullLength : length;

        final long tempFromPosition = position == NULL_POSITION ? metaDecoder.joiningPosition() : position;
        segmentFileIndex = segmentFileIndex(joiningPosition, tempFromPosition, segmentFileLength);
        final long recordingOffset = tempFromPosition & (segmentFileLength - 1);
        openRecordingFile();

        recordingTermStartOffset = (int)(recordingOffset - (recordingOffset & (termBufferLength - 1)));
        termBuffer = new UnsafeBuffer(mappedByteBuffer, recordingTermStartOffset, termBufferLength);
        termOffset = (int)(recordingOffset & (termBufferLength - 1));

        int currFrameOffset = 0;
        while (currFrameOffset < termOffset)
        {
            currFrameOffset += termBuffer.getInt(currFrameOffset + DataHeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET);
        }

        if (currFrameOffset != termOffset)
        {
            fromPosition = tempFromPosition + (currFrameOffset - termOffset);
        }
        else
        {
            fromPosition = tempFromPosition;
        }

        if (currFrameOffset >= termBufferLength)
        {
            termOffset = 0;
            nextTerm();
        }
        else
        {
            termOffset = currFrameOffset;
        }
    }

    boolean isDone()
    {
        return transmitted >= replayLength;
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

        int polled = 0;

        // read to end of term or requested data
        while (termOffset < termBufferLength && !isDone() && polled < fragmentLimit)
        {
            final int currTermOffset = this.termOffset;
            headerFlyweight.wrap(termBuffer, currTermOffset, DataHeaderFlyweight.HEADER_LENGTH);
            final int frameLength = headerFlyweight.frameLength();
            if (frameLength <= 0)
            {
                throw new IllegalStateException("Broken frame with length <= 0: " + headerFlyweight);
            }

            final int alignedLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
            // cursor moves forward, importantly an exception from onFragment will not block progress
            transmitted += alignedLength;
            this.termOffset += alignedLength;

            final int fragmentDataOffset = currTermOffset + DataHeaderFlyweight.DATA_OFFSET;
            final int fragmentDataLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;

            if (!fragmentHandler.onFragment(
                termBuffer,
                fragmentDataOffset,
                fragmentDataLength,
                headerFlyweight))
            {
                // rollback the cursor progress
                transmitted -= alignedLength;
                this.termOffset -= alignedLength;
                return polled;
            }

            // only count data fragments, consistent with sent fragment count
            if (headerFlyweight.headerType() != PADDING_FRAME_TYPE)
            {
                polled++;
            }
        }

        if (!isDone() && termOffset == termBufferLength)
        {
            termOffset = 0;
            nextTerm();
        }

        return polled;
    }

    private void nextTerm() throws IOException
    {
        recordingTermStartOffset += termBufferLength;

        // rotate file
        if (recordingTermStartOffset == segmentFileLength)
        {
            closeRecordingFile();
            segmentFileIndex++;
            openRecordingFile();
        }
        // rotate term
        termBuffer.wrap(mappedByteBuffer, recordingTermStartOffset, termBufferLength);
    }

    private void closeRecordingFile()
    {
        IoUtil.unmap(mappedByteBuffer);
        CloseHelper.close(currentDataChannel);
    }

    private void openRecordingFile() throws IOException
    {
        recordingTermStartOffset = 0;
        final String recordingDataFileName = recordingDataFileName(recordingId, segmentFileIndex);
        final File recordingDataFile = new File(archiveDir, recordingDataFileName);

        if (!recordingDataFile.exists())
        {
            throw new IOException(recordingDataFile.getAbsolutePath() + " not found");
        }

        currentDataChannel = FileChannel.open(recordingDataFile.toPath(), READ);
        mappedByteBuffer = currentDataChannel.map(READ_ONLY, 0, segmentFileLength);
    }

    private static RecordingDescriptorDecoder getDescriptor(final long recordingId, final File archiveDir)
        throws IOException
    {
        final String recordingMetaFileName = recordingMetaFileName(recordingId);
        final File recordingMetaFile = new File(archiveDir, recordingMetaFileName);
        return loadRecordingDescriptor(recordingMetaFile);
    }
}

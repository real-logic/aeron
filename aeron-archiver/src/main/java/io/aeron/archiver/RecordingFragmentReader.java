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
    private final long recordingId;
    private final File archiveDir;
    private final int termBufferLength;
    private final long joiningPosition;
    private final long fullLength;
    private final long fromPosition;
    private final long replayLength;
    private final int segmentFileLength;

    private int segmentFileIndex;
    private FileChannel currentDataChannel = null;
    private UnsafeBuffer termMappedUnsafeBuffer = null;
    private int recordingTermStartOffset;
    private int fragmentOffset;
    private long transmitted = 0;
    private final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();

    RecordingFragmentReader(final long recordingId, final File archiveDir) throws IOException
    {
        this.recordingId = recordingId;
        this.archiveDir = archiveDir;
        final String recordingMetaFileName = recordingMetaFileName(recordingId);
        // TODO: Use metadata from catalog
        final File recordingMetaFile = new File(archiveDir, recordingMetaFileName);
        final RecordingDescriptorDecoder metaDecoder = recordingMetaFileFormatDecoder(recordingMetaFile);
        termBufferLength = metaDecoder.termBufferLength();
        joiningPosition = metaDecoder.joiningPosition();
        segmentFileLength = metaDecoder.segmentFileLength();
        fullLength = ArchiveUtil.recordingFileFullLength(metaDecoder);
        IoUtil.unmap(metaDecoder.buffer().byteBuffer());
        fromPosition = joiningPosition;
        replayLength = fullLength;
        initCursorState();
    }

    RecordingFragmentReader(
        final long recordingId,
        final File archiveDir,
        final long position,
        final long length) throws IOException
    {
        this.recordingId = recordingId;
        this.archiveDir = archiveDir;
        this.fromPosition = position;
        this.replayLength = length;
        final String recordingMetaFileName = recordingMetaFileName(recordingId);
        final File recordingMetaFile = new File(archiveDir, recordingMetaFileName);
        // TODO: Can this be done without mapping and unmapping?
        final RecordingDescriptorDecoder metaDecoder = recordingMetaFileFormatDecoder(recordingMetaFile);
        termBufferLength = metaDecoder.termBufferLength();
        joiningPosition = metaDecoder.joiningPosition();
        segmentFileLength = metaDecoder.segmentFileLength();
        fullLength = ArchiveUtil.recordingFileFullLength(metaDecoder);
        IoUtil.unmap(metaDecoder.buffer().byteBuffer());
        initCursorState();
    }

    private void initCursorState() throws IOException
    {
        segmentFileIndex = segmentFileIndex(joiningPosition, fromPosition, segmentFileLength);
        final long recordingOffset = fromPosition & (segmentFileLength - 1);
        recordingTermStartOffset = (int) (recordingOffset - (recordingOffset & (termBufferLength - 1)));
        openRecordingFile();
        termMappedUnsafeBuffer = new UnsafeBuffer(
            currentDataChannel.map(READ_ONLY, recordingTermStartOffset, termBufferLength));

        // TODO: align first fragment
        fragmentOffset = (int) (recordingOffset & (termBufferLength - 1));
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
        while (fragmentOffset < termBufferLength && !isDone() && polled < fragmentLimit)
        {
            final int fragmentOffset = this.fragmentOffset;
            headerFlyweight.wrap(termMappedUnsafeBuffer, this.fragmentOffset, DataHeaderFlyweight.HEADER_LENGTH);
            final int frameLength = headerFlyweight.frameLength();
            if (frameLength <= 0)
            {
                throw new IllegalStateException("Broken frame with length <= 0: " + headerFlyweight);
            }

            final int alignedLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
            // cursor moves forward, importantly an exception from onFragment will not block progress
            transmitted += alignedLength;
            this.fragmentOffset += alignedLength;



            final int fragmentDataOffset = fragmentOffset + DataHeaderFlyweight.DATA_OFFSET;
            final int fragmentDataLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;

            if (!fragmentHandler.onFragment(
                termMappedUnsafeBuffer,
                fragmentDataOffset,
                fragmentDataLength,
                headerFlyweight))
            {
                // rollback the cursor progress
                transmitted -= alignedLength;
                this.fragmentOffset -= alignedLength;
                return polled;
            }

            // only count data fragments, consistent with sent fragment count
            if (headerFlyweight.headerType() != PADDING_FRAME_TYPE)
            {
                polled++;
            }

        }

        if (!isDone() && fragmentOffset == termBufferLength)
        {
            fragmentOffset = 0;
            recordingTermStartOffset += termBufferLength;

            // rotate file
            if (recordingTermStartOffset == segmentFileLength)
            {
                closeRecordingFile();
                segmentFileIndex++;
                openRecordingFile();
                recordingTermStartOffset = 0;
            }
            else
            {
                IoUtil.unmap(termMappedUnsafeBuffer.byteBuffer());
            }
            // TODO: Is a mapping created per term? Would it not be more efficient to do it per segment?
            // rotate term
            final MappedByteBuffer mappedByteBuffer = currentDataChannel.map(
                READ_ONLY, recordingTermStartOffset, termBufferLength);
            termMappedUnsafeBuffer.wrap(mappedByteBuffer);
        }

        return polled;
    }

    private void closeRecordingFile()
    {
        IoUtil.unmap(termMappedUnsafeBuffer.byteBuffer());
        CloseHelper.close(currentDataChannel);
    }

    private void openRecordingFile() throws IOException
    {
        final String recordingDataFileName = recordingDataFileName(recordingId, segmentFileIndex);
        final File recordingDataFile = new File(archiveDir, recordingDataFileName);

        if (!recordingDataFile.exists())
        {
            throw new IOException(recordingDataFile.getAbsolutePath() + " not found");
        }

        currentDataChannel = FileChannel.open(recordingDataFile.toPath(), READ);
    }

    boolean isDone()
    {
        return transmitted >= replayLength;
    }

    public void close()
    {
        closeRecordingFile();
    }

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
}

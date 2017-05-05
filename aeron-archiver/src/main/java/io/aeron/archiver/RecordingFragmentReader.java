/*
 * Copyright 2014-2017 Real Logic Ltd.
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
 *
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
    private final int recordingId;
    private final File archiveFolder;
    private final int initialTermId;
    private final int termBufferLength;
    private final int initialTermOffset;
    private final long fullLength;
    private final int fromTermId;
    private final int fromTermOffset;
    private final long replayLength;
    private final int recordingFileLength;

    private int recordingFileIndex;
    private FileChannel currentDataChannel = null;
    private UnsafeBuffer termMappedUnsafeBuffer = null;
    private int recordingTermStartOffset;
    private int fragmentOffset;
    private long transmitted = 0;
    private final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();

    RecordingFragmentReader(final int recordingId, final File archiveFolder) throws IOException
    {
        this.recordingId = recordingId;
        this.archiveFolder = archiveFolder;
        final String recordingMetaFileName = recordingMetaFileName(recordingId);
        // TODO: Should this just be read rather than mapped given the one of read?
        final File recordingMetaFile = new File(archiveFolder, recordingMetaFileName);
        final RecordingDescriptorDecoder metaDecoder = recordingMetaFileFormatDecoder(recordingMetaFile);
        termBufferLength = metaDecoder.termBufferLength();
        initialTermId = metaDecoder.initialTermId();
        initialTermOffset = metaDecoder.initialTermOffset();
        recordingFileLength = metaDecoder.fileLength();
        fullLength = ArchiveUtil.recordingFileFullLength(metaDecoder);
        IoUtil.unmap(metaDecoder.buffer().byteBuffer());
        fromTermId = initialTermId;
        fromTermOffset = initialTermOffset;
        replayLength = fullLength;
        initCursorState();
    }

    RecordingFragmentReader(
        final int recordingId,
        final File archiveFolder,
        final int termId,
        final int termOffset,
        final long length) throws IOException
    {
        this.recordingId = recordingId;
        this.archiveFolder = archiveFolder;
        this.fromTermId = termId;
        this.fromTermOffset = termOffset;
        this.replayLength = length;
        final String recordingMetaFileName = recordingMetaFileName(recordingId);
        final File recordingMetaFile = new File(archiveFolder, recordingMetaFileName);
        final RecordingDescriptorDecoder metaDecoder = recordingMetaFileFormatDecoder(recordingMetaFile);
        termBufferLength = metaDecoder.termBufferLength();
        initialTermId = metaDecoder.initialTermId();
        initialTermOffset = metaDecoder.initialTermOffset();
        recordingFileLength = metaDecoder.fileLength();
        fullLength = ArchiveUtil.recordingFileFullLength(metaDecoder);
        IoUtil.unmap(metaDecoder.buffer().byteBuffer());
        initCursorState();
    }

    private void initCursorState() throws IOException
    {
        recordingFileIndex = recordingDataFileIndex(initialTermId, termBufferLength, fromTermId, recordingFileLength);
        final int archiveOffset = offsetInArchivedFile(
            fromTermOffset, fromTermId, initialTermId, termBufferLength, recordingFileLength);
        recordingTermStartOffset = archiveOffset - fromTermOffset;
        openRecordingFile();
        termMappedUnsafeBuffer = new UnsafeBuffer(
            currentDataChannel.map(READ_ONLY, recordingTermStartOffset, termBufferLength));

        // TODO: align first fragment
        fragmentOffset = archiveOffset & (termBufferLength - 1);
    }

    int controlledPoll(final SimplifiedControlledPoll fragmentHandler, final int fragmentLimit) throws IOException
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

            if (headerFlyweight.headerType() == PADDING_FRAME_TYPE)
            {
                continue;
            }

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
            // only count data fragments
            polled++;
        }

        if (!isDone() && fragmentOffset == termBufferLength)
        {
            fragmentOffset = 0;
            recordingTermStartOffset += termBufferLength;

            // rotate file
            if (recordingTermStartOffset == recordingFileLength)
            {
                closeRecordingFile();
                recordingFileIndex++;
                openRecordingFile();
                recordingTermStartOffset = 0;
            }
            else
            {
                unmapTermBuffer();
            }
            // rotate term
            final MappedByteBuffer mappedByteBuffer = currentDataChannel.map(
                READ_ONLY, recordingTermStartOffset, termBufferLength);
            termMappedUnsafeBuffer.wrap(mappedByteBuffer);
        }

        return polled;
    }

    private void unmapTermBuffer()
    {
        if (termMappedUnsafeBuffer != null)
        {
            IoUtil.unmap(termMappedUnsafeBuffer.byteBuffer());
        }
    }

    private void closeRecordingFile()
    {
        unmapTermBuffer();
        CloseHelper.close(currentDataChannel);
    }

    private void openRecordingFile() throws IOException
    {
        final String recordingDataFileName = recordingDataFileName(recordingId, recordingFileIndex);
        final File recordingDataFile = new File(archiveFolder, recordingDataFileName);

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
        boolean onFragment(
            DirectBuffer fragmentBuffer,
            int fragmentOffset,
            int fragmentLength,
            DataHeaderFlyweight header);
    }
}

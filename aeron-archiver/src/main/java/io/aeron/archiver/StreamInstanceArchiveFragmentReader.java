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


import io.aeron.archiver.messages.ArchiveDescriptorDecoder;
import io.aeron.logbuffer.*;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.channels.FileChannel;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.PADDING_FRAME_TYPE;

class StreamInstanceArchiveFragmentReader implements AutoCloseable
{
    private final int streamInstanceId;
    private final File archiveFolder;
    private final int initialTermId;
    private final int termBufferLength;
    private final int initialTermOffset;
    private final long fullLength;
    private final int fromTermId;
    private final int fromTermOffset;
    private final long replayLength;

    private int archiveFileIndex;
    private RandomAccessFile currentDataFile = null;
    private FileChannel currentDataChannel = null;
    private UnsafeBuffer termMappedUnsafeBuffer = null;
    private int archiveTermStartOffset;
    private int fragmentOffset;
    private Header fragmentHeader;
    private long transmitted = 0;

    StreamInstanceArchiveFragmentReader(final int streamInstanceId, final File archiveFolder) throws IOException
    {
        this.streamInstanceId = streamInstanceId;
        this.archiveFolder = archiveFolder;
        final String archiveMetaFileName = ArchiveFileUtil.archiveMetaFileName(streamInstanceId);
        final File archiveMetaFile = new File(archiveFolder, archiveMetaFileName);
        final ArchiveDescriptorDecoder metaDecoder =
            ArchiveFileUtil.archiveMetaFileFormatDecoder(archiveMetaFile);
        termBufferLength = metaDecoder.termBufferLength();
        initialTermId = metaDecoder.initialTermId();
        initialTermOffset = metaDecoder.initialTermOffset();
        fullLength = ArchiveFileUtil.archiveFullLength(metaDecoder);
        IoUtil.unmap(metaDecoder.buffer().byteBuffer());
        fromTermId = initialTermId;
        fromTermOffset = initialTermOffset;
        replayLength = fullLength;
        initCursorState();
    }

    StreamInstanceArchiveFragmentReader(
        final int streamInstanceId,
        final File archiveFolder,
        final int termId,
        final int termOffset,
        final long length) throws IOException
    {
        this.streamInstanceId = streamInstanceId;
        this.archiveFolder = archiveFolder;
        this.fromTermId = termId;
        this.fromTermOffset = termOffset;
        this.replayLength = length;
        final String archiveMetaFileName = ArchiveFileUtil.archiveMetaFileName(streamInstanceId);
        final File archiveMetaFile = new File(archiveFolder, archiveMetaFileName);
        final ArchiveDescriptorDecoder metaDecoder =
            ArchiveFileUtil.archiveMetaFileFormatDecoder(archiveMetaFile);
        termBufferLength = metaDecoder.termBufferLength();
        initialTermId = metaDecoder.initialTermId();
        initialTermOffset = metaDecoder.initialTermOffset();
        fullLength = ArchiveFileUtil.archiveFullLength(metaDecoder);
        IoUtil.unmap(metaDecoder.buffer().byteBuffer());
        initCursorState();
    }

    private void initCursorState() throws IOException
    {
        archiveFileIndex = ArchiveFileUtil.archiveDataFileIndex(initialTermId, termBufferLength, fromTermId);
        final int archiveOffset =
            ArchiveFileUtil.archiveOffset(fromTermOffset, fromTermId, initialTermId, termBufferLength);
        archiveTermStartOffset = archiveOffset - fromTermOffset;
        openArchiveFile();
        termMappedUnsafeBuffer =
            new UnsafeBuffer(currentDataChannel.map(FileChannel.MapMode.READ_ONLY,
                archiveTermStartOffset,
                termBufferLength));

        fragmentHeader = new Header(initialTermId, Integer.numberOfLeadingZeros(termBufferLength));
        fragmentHeader.buffer(termMappedUnsafeBuffer);

        // TODO: align first fragment
        fragmentOffset = archiveOffset & (termBufferLength - 1);
    }

    int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit) throws IOException
    {
        if (isDone())
        {
            return 0;
        }

        int polled = 0;


        // read to end of term or requested data
        while (fragmentOffset < termBufferLength &&
               !isDone() &&
               polled < fragmentLimit)
        {
            fragmentHeader.offset(fragmentOffset);
            final int frameLength = fragmentHeader.frameLength();
            if (frameLength <= 0)
            {
                final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
                headerFlyweight.wrap(termMappedUnsafeBuffer, fragmentOffset, DataHeaderFlyweight.HEADER_LENGTH);
                throw new IllegalStateException("Broken frame with length <= 0: " + headerFlyweight);
            }

            if (fragmentHeader.type() == PADDING_FRAME_TYPE)
            {
                return 0;
            }

            final int fragmentDataOffset = fragmentOffset + DataHeaderFlyweight.DATA_OFFSET;
            final int fragmentDataLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;
            final Action action = fragmentHandler.onFragment(
                termMappedUnsafeBuffer,
                fragmentDataOffset,
                fragmentDataLength,
                fragmentHeader);
            if (action.equals(Action.ABORT))
            {
                return polled;
            }
            // move to next fragment
            polled++;
            final int alignedLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
            transmitted += alignedLength;
            fragmentOffset += alignedLength;
        }

        if (!isDone() && fragmentOffset == termBufferLength)
        {
            fragmentOffset = 0;
            archiveTermStartOffset += termBufferLength;

            // rotate file
            if (archiveTermStartOffset == ArchiveFileUtil.ARCHIVE_FILE_SIZE)
            {
                closeArchiveFile();
                archiveFileIndex++;
                openArchiveFile();
                archiveTermStartOffset = 0;
            }
            else
            {
                unmapTermBuffer();
            }
            // rotate term
            termMappedUnsafeBuffer.wrap(currentDataChannel.map(
                FileChannel.MapMode.READ_ONLY,
                archiveTermStartOffset,
                termBufferLength));
            fragmentHeader.buffer(termMappedUnsafeBuffer);
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

    private void closeArchiveFile()
    {
        unmapTermBuffer();
        CloseHelper.quietClose(currentDataChannel);
        CloseHelper.quietClose(currentDataFile);
    }

    private void openArchiveFile() throws IOException
    {
        final String archiveDataFileName =
            ArchiveFileUtil.archiveDataFileName(streamInstanceId, archiveFileIndex);
        final File archiveDataFile = new File(archiveFolder, archiveDataFileName);

        if (!archiveDataFile.exists())
        {
            throw new IOException(archiveDataFile.getAbsolutePath() + " not found");
        }

        currentDataFile = new RandomAccessFile(archiveDataFile, "r");
        currentDataChannel = currentDataFile.getChannel();
    }

    boolean isDone()
    {
        return transmitted >= replayLength;
    }

    private int readFragment(
        final ControlledFragmentHandler fragmentHandler,
        final UnsafeBuffer termMappedUnsafeBuffer,
        final int fragmentOffset,
        final int frameLength,
        final Header fragmentHeader)
    {
        if (frameLength <= 0)
        {
            final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
            headerFlyweight.wrap(termMappedUnsafeBuffer, fragmentOffset, DataHeaderFlyweight.HEADER_LENGTH);
            throw new IllegalStateException("Broken frame with length <= 0: " + headerFlyweight);
        }

        if (fragmentHeader.type() == PADDING_FRAME_TYPE)
        {
            return 0;
        }

        final int fragmentDataOffset = fragmentOffset + DataHeaderFlyweight.DATA_OFFSET;
        final int fragmentDataLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;
        final Action action = fragmentHandler.onFragment(
            termMappedUnsafeBuffer,
            fragmentDataOffset,
            fragmentDataLength,
            fragmentHeader);
        if (action.equals(Action.ABORT))
        {
            return -1;
        }
        else
        {
            return 1;
        }


    }

    public void close()
    {
        closeArchiveFile();
    }
}

/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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


import io.aeron.archiver.messages.ArchiveMetaFileFormatDecoder;
import io.aeron.logbuffer.*;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.channels.FileChannel;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.PADDING_FRAME_TYPE;

/**
 * TODO: Make {@link AutoCloseable}
 * TODO: Is this useful beyond testing?
 * TODO: More closely reflect {@link io.aeron.Image} API
 */
class StreamInstanceArchiveFragmentReader
{
    private final int streamInstanceId;
    private final File archiveFolder;
    private final int initialTermId;
    private final int termBufferLength;
    private final int initialTermOffset;
    private final int lastTermId;
    private final int lastTermOffset;
    private final long fullLength;
    private final int termId;
    private final int termOffset;
    private final long length;

    StreamInstanceArchiveFragmentReader(final int streamInstanceId, final File archiveFolder) throws IOException
    {
        this.streamInstanceId = streamInstanceId;
        this.archiveFolder = archiveFolder;
        final String archiveMetaFileName = ArchiveFileUtil.archiveMetaFileName(streamInstanceId);
        final File archiveMetaFile = new File(archiveFolder, archiveMetaFileName);
        final ArchiveMetaFileFormatDecoder metaDecoder =
            ArchiveFileUtil.archiveMetaFileFormatDecoder(archiveMetaFile);
        termBufferLength = metaDecoder.termBufferLength();
        initialTermId = metaDecoder.initialTermId();
        initialTermOffset = metaDecoder.initialTermOffset();
        lastTermId = metaDecoder.lastTermId();
        lastTermOffset = metaDecoder.lastTermOffset();
        fullLength = ArchiveFileUtil.archiveFullLength(metaDecoder);
        IoUtil.unmap(metaDecoder.buffer().byteBuffer());
        termId = initialTermId;
        termOffset = initialTermOffset;
        length = fullLength;
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
        this.termId = termId;
        this.termOffset = termOffset;
        this.length = length;
        final String archiveMetaFileName = ArchiveFileUtil.archiveMetaFileName(streamInstanceId);
        final File archiveMetaFile = new File(archiveFolder, archiveMetaFileName);
        final ArchiveMetaFileFormatDecoder metaDecoder =
            ArchiveFileUtil.archiveMetaFileFormatDecoder(archiveMetaFile);
        termBufferLength = metaDecoder.termBufferLength();
        initialTermId = metaDecoder.initialTermId();
        initialTermOffset = metaDecoder.initialTermOffset();
        lastTermId = metaDecoder.lastTermId();
        lastTermOffset = metaDecoder.lastTermOffset();
        fullLength = ArchiveFileUtil.archiveFullLength(metaDecoder);
        IoUtil.unmap(metaDecoder.buffer().byteBuffer());
    }

    int poll(final FragmentHandler fragmentHandler) throws IOException
    {
        return poll(fragmentHandler, initialTermId, initialTermOffset, fullLength);
    }

    int poll(
        final FragmentHandler fragmentHandler,
        final int fromTermId,
        final int fromTermOffset,
        final long replayLength) throws IOException
    {
        int polled = 0;
        long transmitted = 0;
        int archiveFileIndex = ArchiveFileUtil.archiveDataFileIndex(initialTermId, termBufferLength, fromTermId);
        final int archiveOffset =
            ArchiveFileUtil.archiveOffset(fromTermOffset, fromTermId, initialTermId, termBufferLength);
        final String archiveDataFileName =
            ArchiveFileUtil.archiveDataFileName(streamInstanceId, archiveFileIndex);
        final File archiveDataFile = new File(archiveFolder, archiveDataFileName);

        if (!archiveDataFile.exists())
        {
            throw new IllegalStateException(archiveDataFile.getAbsolutePath() + " not found");
        }

        RandomAccessFile currentDataFile = null;
        FileChannel currentDataChannel = null;
        UnsafeBuffer termMappedUnsafeBuffer = null;
        try
        {
            currentDataFile = new RandomAccessFile(archiveDataFile, "r");
            currentDataChannel = currentDataFile.getChannel();
            int archiveTermStartOffset = archiveOffset - fromTermOffset;
            termMappedUnsafeBuffer =
                new UnsafeBuffer(currentDataChannel.map(FileChannel.MapMode.READ_ONLY,
                    archiveTermStartOffset,
                    termBufferLength));
            int fragmentOffset = archiveOffset & (termBufferLength - 1);
            while (true)
            {
                final Header fragmentHeader =
                    new Header(initialTermId, Integer.numberOfLeadingZeros(termBufferLength));
                fragmentHeader.buffer(termMappedUnsafeBuffer);

                // read to end of term or requested data
                while (fragmentOffset < termBufferLength && transmitted < replayLength)
                {
                    fragmentHeader.offset(fragmentOffset);
                    final int frameLength = fragmentHeader.frameLength();
                    polled += readFragment(
                        fragmentHandler,
                        termMappedUnsafeBuffer,
                        fragmentOffset,
                        frameLength,
                        fragmentHeader);
                    final int alignedLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
                    transmitted += alignedLength;
                    fragmentOffset += alignedLength;
                }

                if (transmitted >= replayLength)
                {
                    return polled;
                }
                fragmentOffset = 0;
                archiveTermStartOffset += termBufferLength;
                if (archiveTermStartOffset == ArchiveFileUtil.ARCHIVE_FILE_SIZE)
                {
                    archiveTermStartOffset = 0;
                    archiveFileIndex++;
                    final String archiveDataFileNameN =
                        ArchiveFileUtil.archiveDataFileName(streamInstanceId, archiveFileIndex);
                    final File archiveDataFileN = new File(archiveFolder, archiveDataFileNameN);

                    if (!archiveDataFileN.exists())
                    {
                        throw new IllegalStateException(archiveDataFileN.getAbsolutePath() + " not found");
                    }
                    CloseHelper.quietClose(currentDataFile);
                    CloseHelper.quietClose(currentDataChannel);

                    currentDataFile = new RandomAccessFile(archiveDataFileN, "r");
                    currentDataChannel = currentDataFile.getChannel();
                }
                // roll term
                IoUtil.unmap(termMappedUnsafeBuffer.byteBuffer());
                termMappedUnsafeBuffer.wrap(currentDataChannel.map(FileChannel.MapMode.READ_ONLY,
                    archiveTermStartOffset,
                    termBufferLength));
            }
        }
        finally
        {
            CloseHelper.quietClose(currentDataFile);
            CloseHelper.quietClose(currentDataChannel);
            if (termMappedUnsafeBuffer != null)
            {
                IoUtil.unmap(termMappedUnsafeBuffer.byteBuffer());
            }
        }
    }

    private int readFragment(
        final FragmentHandler fragmentHandler,
        final UnsafeBuffer termMappedUnsafeBuffer,
        final int fragmentOffset,
        final int frameLength, final Header fragmentHeader)
    {
        if (frameLength <= 0)
        {
            final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
            headerFlyweight.wrap(termMappedUnsafeBuffer, fragmentOffset, DataHeaderFlyweight.HEADER_LENGTH);
            throw new IllegalStateException("Broken frame with length <= 0: " + headerFlyweight);
        }
        if (fragmentHeader.type() != PADDING_FRAME_TYPE)
        {
            final int fragmentDataOffset = fragmentOffset + DataHeaderFlyweight.DATA_OFFSET;
            final int fragmentDataLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;
            fragmentHandler.onFragment(
                termMappedUnsafeBuffer,
                fragmentDataOffset,
                fragmentDataLength,
                fragmentHeader);
            return 1;
        }
        return 0;
    }
}

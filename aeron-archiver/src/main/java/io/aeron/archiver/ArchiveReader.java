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
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.logbuffer.FrameDescriptor.PADDING_FRAME_TYPE;

public class ArchiveReader
{
    private final int streamInstanceId;
    private final File archiveFolder;
    private final int initialTermId;
    private final int termBufferLength;
    private final int initialTermOffset;
    private final int lastTermId;
    private final int lastTermOffset;

    ArchiveReader(final int streamInstanceId, final File archiveFolder) throws IOException
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

        final int lastTermId = metaDecoder.lastTermId();

    }

    void forEachFragment(final FragmentHandler fragmentHandler,
                         final int termId,
                         final int termOffset,
                         final long length) throws IOException
    {
        long transmitted = 0;
        int archiveFileIndex = ArchiveFileUtil.archiveDataFileIndex(initialTermId, termBufferLength, termId);
        final int archiveOffset = ArchiveFileUtil.archiveOffset(termOffset, termId, initialTermId, termBufferLength);
        final String archiveDataFileName =
            ArchiveFileUtil.archiveDataFileName(streamInstanceId, archiveFileIndex);
        final File archiveDataFile = new File(archiveFolder, archiveDataFileName);

        if (!archiveDataFile.exists())
        {
            throw new IllegalStateException(archiveDataFile.getAbsolutePath() + " not found");
        }

        RandomAccessFile currentDataFile = new RandomAccessFile(archiveDataFile, "r");
        FileChannel currentDataChannel = currentDataFile.getChannel();
        try
        {
            int archiveTermStartOffset = archiveOffset - termOffset;
            MappedByteBuffer termMappedBuffer = currentDataChannel.map(FileChannel.MapMode.READ_ONLY,
                                                                       archiveTermStartOffset,
                                                                       termBufferLength);

            UnsafeBuffer termMappedUnsafeBuffer = new UnsafeBuffer(termMappedBuffer);
            int fragmentOffset = archiveOffset & (termBufferLength - 1);
            while (true)
            {
                final Header fragmentHeader =
                    new Header(initialTermId, Integer.numberOfTrailingZeros(termBufferLength));
                fragmentHeader.buffer(termMappedUnsafeBuffer);

                while (fragmentOffset < termBufferLength && transmitted < length)
                {
                    fragmentHeader.offset(fragmentOffset);
                    final int frameLength = fragmentHeader.frameLength();
                    if (fragmentHeader.type() != PADDING_FRAME_TYPE)
                    {
                        final int fragmentDataOffset = fragmentOffset + DataHeaderFlyweight.DATA_OFFSET;
                        final int fragmentDataLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;
                        fragmentHandler.onFragment(termMappedUnsafeBuffer,
                                                   fragmentDataOffset,
                                                   fragmentDataLength,
                                                   fragmentHeader);
                    }
                    fragmentOffset += frameLength;
                    transmitted +=  frameLength;
                }
                if (transmitted >= length)
                {
                    return;
                }
                fragmentOffset = 0;
                archiveTermStartOffset += termBufferLength;
                if (archiveTermStartOffset == ArchiveFileUtil.ARCHIVE_FILE_SIZE)
                {
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

                    currentDataFile = new RandomAccessFile(archiveDataFile, "r");
                    currentDataChannel = currentDataFile.getChannel();
                }
                termMappedBuffer = currentDataChannel.map(FileChannel.MapMode.READ_ONLY,
                                                          archiveTermStartOffset,
                                                          termBufferLength);

                termMappedUnsafeBuffer = new UnsafeBuffer(termMappedBuffer);
            }
        }
        finally
        {
            CloseHelper.quietClose(currentDataFile);
            CloseHelper.quietClose(currentDataChannel);
        }
    }
}

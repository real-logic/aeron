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

import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.channels.FileChannel;

import static java.lang.Math.min;

class ArchiveDataChunkReadingCursor implements AutoCloseable
{
    private final int streamInstanceId;
    private final File archiveFolder;
    private final int termBufferLength;

    private final long length;

    private long transmitted;
    private int archiveFileIndex;

    private RandomAccessFile currentDataFile;
    private FileChannel currentDataChannel;
    private UnsafeBuffer termMappedUnsafeBuffer;

    private int archiveTermStartOffset;
    private int currentTermOffset;

    ArchiveDataChunkReadingCursor(final int streamInstanceId,
                                  final File archiveFolder,
                                  final int initialTermId,
                                  final int termBufferLength,
                                  final int termId,
                                  final int termOffset,
                                  final long length)
    {
        this.streamInstanceId = streamInstanceId;
        this.archiveFolder = archiveFolder;
        this.termBufferLength = termBufferLength;

        this.length = length;

        transmitted = 0;
        archiveFileIndex = ArchiveFileUtil.archiveDataFileIndex(initialTermId, termBufferLength, termId);
        final int archiveOffset =
            ArchiveFileUtil.archiveOffset(termOffset, termId, initialTermId, termBufferLength);
        final String archiveDataFileName =
            ArchiveFileUtil.archiveDataFileName(streamInstanceId, archiveFileIndex);
        final File archiveDataFile = new File(archiveFolder, archiveDataFileName);

        if (!archiveDataFile.exists())
        {
            throw new IllegalStateException(archiveDataFile.getAbsolutePath() + " not found");
        }

        currentDataFile = null;
        try
        {
            currentDataFile = new RandomAccessFile(archiveDataFile, "r");
        }
        catch (FileNotFoundException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
        currentDataChannel = currentDataFile.getChannel();
        archiveTermStartOffset = archiveOffset - termOffset;
        currentTermOffset = termOffset;
        try
        {
            termMappedUnsafeBuffer =
                new UnsafeBuffer(currentDataChannel.map(FileChannel.MapMode.READ_ONLY,
                                                        archiveTermStartOffset,
                                                        termBufferLength));
        }
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    boolean isDone()
    {
        return transmitted == length;
    }

    int readChunk(final ArchiveDataFileReader.ChunkHandler handler, final int chunkLength)
    {
        final int remainingInTerm = termBufferLength - currentTermOffset;
        final long remainingInCursor = length - transmitted;
        final int readSize = (int) min(chunkLength, min(remainingInTerm, remainingInCursor));

        if (readSize == 0)
        {
            return 0;
        }

        if (!handler.handle(termMappedUnsafeBuffer, currentTermOffset, readSize))
        {
            return 0;
        }
        currentTermOffset += readSize;
        transmitted += readSize;
        if (currentTermOffset == termBufferLength)
        {
            try
            {
                rollNextTerm();
            }
            catch (IOException e)
            {
                LangUtil.rethrowUnchecked(e);
            }
        }
        return readSize;
    }

    private void rollNextTerm() throws IOException
    {
        currentTermOffset = 0;
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
            IoUtil.unmap(termMappedUnsafeBuffer.byteBuffer());

            currentDataFile = new RandomAccessFile(archiveDataFileN, "r");
            currentDataChannel = currentDataFile.getChannel();
        }
        // roll term
        termMappedUnsafeBuffer.wrap(currentDataChannel.map(FileChannel.MapMode.READ_ONLY,
                                                           archiveTermStartOffset,
                                                           termBufferLength));
    }

    @Override
    public void close() throws Exception
    {
        CloseHelper.quietClose(currentDataFile);
        CloseHelper.quietClose(currentDataChannel);
        IoUtil.unmap(termMappedUnsafeBuffer.byteBuffer());
    }
}

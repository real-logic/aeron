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
import java.util.Objects;

import static java.lang.Math.min;

class StreamInstanceArchiveChunkReader implements AutoCloseable
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

    StreamInstanceArchiveChunkReader(
        final int streamInstanceId,
        final File archiveFolder,
        final int initialTermId,
        final int termBufferLength,
        final int termId,
        final int termOffset,
        final long length) throws IOException
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
            throw new IOException(archiveDataFile.getAbsolutePath() + " not found");
        }

        try
        {
            currentDataFile = new RandomAccessFile(archiveDataFile, "r");
            currentDataChannel = currentDataFile.getChannel();
            archiveTermStartOffset = archiveOffset - termOffset;
            currentTermOffset = termOffset;
            termMappedUnsafeBuffer = new UnsafeBuffer(
                currentDataChannel.map(FileChannel.MapMode.READ_ONLY, archiveTermStartOffset, termBufferLength));
        }
        catch (IOException e)
        {
            CloseHelper.quietClose(this);
            throw e;
        }
    }

    boolean isDone()
    {
        return transmitted == length;
    }

    int readChunk(final ChunkHandler handler, final int chunkLength)
    {
        if (chunkLength <= 0)
        {
            throw new IllegalArgumentException("chunkLength <= 0: " + chunkLength);
        }
        Objects.requireNonNull(handler);

        final int remainingInTerm = termBufferLength - currentTermOffset;
        final long remainingInCursor = length - transmitted;
        final int readSize = (int)min(chunkLength, min(remainingInTerm, remainingInCursor));

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
        if (transmitted != length && currentTermOffset == termBufferLength)
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

            closeResources();

            currentDataFile = new RandomAccessFile(archiveDataFileN, "r");
            currentDataChannel = currentDataFile.getChannel();
        }
        // roll term
        termMappedUnsafeBuffer.wrap(currentDataChannel.map(
            FileChannel.MapMode.READ_ONLY, archiveTermStartOffset, termBufferLength));
    }

    public void close()
    {
        closeResources();
    }

    private void closeResources()
    {
        IoUtil.unmap(termMappedUnsafeBuffer.byteBuffer());
        CloseHelper.quietClose(currentDataChannel);
        CloseHelper.quietClose(currentDataFile);
    }

    interface ChunkHandler
    {
        boolean handle(UnsafeBuffer buffer, int offset, int length);
    }
}

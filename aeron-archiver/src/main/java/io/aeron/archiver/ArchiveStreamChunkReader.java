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

import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Objects;

import static java.lang.Math.min;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;

class ArchiveStreamChunkReader implements AutoCloseable
{
    private final int streamInstanceId;
    private final File archiveFolder;
    private final int termBufferLength;

    private final long length;

    private long transmitted;
    private int fileIndex;

    private FileChannel dataChannel;
    private UnsafeBuffer termBuffer;

    private int initTermOffset;
    private int termOffset;

    ArchiveStreamChunkReader(
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
        fileIndex = ArchiveFileUtil.archiveDataFileIndex(initialTermId, termBufferLength, termId);
        final int archiveOffset = ArchiveFileUtil.archiveOffset(termOffset, termId, initialTermId, termBufferLength);
        initTermOffset = archiveOffset - termOffset;
        this.termOffset = termOffset;

        final String archiveDataFileName = ArchiveFileUtil.archiveDataFileName(streamInstanceId, fileIndex);
        final File archiveDataFile = new File(archiveFolder, archiveDataFileName);

        if (!archiveDataFile.exists())
        {
            throw new IOException(archiveDataFile.getAbsolutePath() + " not found");
        }

        try
        {
            dataChannel = FileChannel.open(archiveDataFile.toPath(), READ);
            termBuffer = new UnsafeBuffer(dataChannel.map(READ_ONLY, initTermOffset, termBufferLength));
        }
        catch (final IOException ex)
        {
            CloseHelper.quietClose(this);
            throw ex;
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

        final int remainingInTerm = termBufferLength - termOffset;
        final long remainingInCursor = length - transmitted;
        final int readSize = (int)min(chunkLength, min(remainingInTerm, remainingInCursor));

        if (readSize == 0)
        {
            return 0;
        }

        if (!handler.handle(termBuffer, termOffset, readSize))
        {
            return 0;
        }

        termOffset += readSize;
        transmitted += readSize;
        if (transmitted != length && termOffset >= termBufferLength)
        {
            try
            {
                rollNextTerm();
            }
            catch (final IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        return readSize;
    }

    private void rollNextTerm() throws IOException
    {
        termOffset = 0;
        initTermOffset += termBufferLength;
        if (initTermOffset == ArchiveFileUtil.ARCHIVE_FILE_SIZE)
        {
            initTermOffset = 0;
            fileIndex++;
            final String archiveDataFileNameN = ArchiveFileUtil.archiveDataFileName(streamInstanceId, fileIndex);
            final File archiveDataFileN = new File(archiveFolder, archiveDataFileNameN);

            if (!archiveDataFileN.exists())
            {
                throw new IllegalStateException(archiveDataFileN.getAbsolutePath() + " not found");
            }

            closeResources();

            dataChannel = FileChannel.open(archiveDataFileN.toPath(), READ);
        }
        else
        {
            IoUtil.unmap(termBuffer.byteBuffer());
        }

        // roll term
        termBuffer.wrap(dataChannel.map(READ_ONLY, initTermOffset, termBufferLength));
    }

    public void close()
    {
        closeResources();
    }

    private void closeResources()
    {
        IoUtil.unmap(termBuffer.byteBuffer());
        CloseHelper.quietClose(dataChannel);
    }

    interface ChunkHandler
    {
        boolean handle(UnsafeBuffer buffer, int offset, int length);
    }
}

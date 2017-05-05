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

import static io.aeron.archiver.ArchiveUtil.offsetInSegmentFile;
import static io.aeron.archiver.ArchiveUtil.segmentFileIndex;
import static java.lang.Math.min;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;

class RecordingChunkReader implements AutoCloseable
{
    private final int recordingId;
    private final File archiveDir;
    private final int termBufferLength;
    private final int archiveFileSize;

    private final long length;

    private long transmitted;
    private int fileIndex;

    private FileChannel dataChannel;
    private UnsafeBuffer termBuffer;

    private int initTermOffset;
    private int termOffset;

    RecordingChunkReader(
        final int recordingId,
        final File archiveDir,
        final int initialTermId,
        final int termBufferLength,
        final int termId,
        final int termOffset,
        final long length,
        final int archiveFileSize) throws IOException
    {
        this.archiveFileSize = archiveFileSize;
        this.recordingId = recordingId;
        this.archiveDir = archiveDir;
        this.termBufferLength = termBufferLength;

        this.length = length;

        transmitted = 0;
        fileIndex = segmentFileIndex(initialTermId, termBufferLength, termId, archiveFileSize);
        final int archiveOffset = offsetInSegmentFile(
            termOffset,
            termId,
            initialTermId,
            termBufferLength,
            archiveFileSize);
        initTermOffset = archiveOffset - termOffset;
        this.termOffset = termOffset;

        final String archiveDataFileName = ArchiveUtil.recordingDataFileName(recordingId, fileIndex);
        final File archiveDataFile = new File(archiveDir, archiveDataFileName);

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
        if (initTermOffset == archiveFileSize)
        {
            initTermOffset = 0;
            fileIndex++;
            final String archiveDataFileNameN = ArchiveUtil.recordingDataFileName(recordingId, fileIndex);
            final File archiveDataFileN = new File(archiveDir, archiveDataFileNameN);

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
        CloseHelper.close(dataChannel);
    }

    interface ChunkHandler
    {
        boolean handle(UnsafeBuffer buffer, int offset, int length);
    }
}

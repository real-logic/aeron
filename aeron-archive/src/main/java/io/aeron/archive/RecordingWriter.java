/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.archive;

import io.aeron.Counter;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.BlockHandler;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;

import static io.aeron.archive.Archive.segmentFileIndex;
import static io.aeron.archive.Archive.segmentFileName;

/**
 * Responsible for writing out a recording into the file system. A recording has descriptor file and a set of data files
 * written into the archive folder.
 * <p>
 * <b>Design note:</b> While this class is notionally closely related to the {@link RecordingSession} it is separated
 * from it for the following reasons:
 * <ul>
 * <li>Easier testing and in particular simplified re-use in testing.</li>
 * <li>Isolation of an external relationship, namely the file system.</li>
 * <li>While a {@link RecordingWriter} is part of a {@link RecordingSession}, a session may transition without actually
 * creating a {@link RecordingWriter}.</li>
 * </ul>
 */
class RecordingWriter implements BlockHandler
{
    private final long recordingId;
    private final long startPosition;
    private final int segmentFileLength;
    private final boolean forceWrites;
    private final boolean forceMetadata;
    private final Counter recordedPosition;
    private final FileChannel archiveDirChannel;
    private final File archiveDir;

    private int segmentPosition;
    private int segmentIndex;
    private FileChannel recordingFileChannel;

    private boolean isClosed = false;

    RecordingWriter(
        final long recordingId,
        final long startPosition,
        final long joinPosition,
        final int termBufferLength,
        final Archive.Context context,
        final FileChannel archiveDirChannel,
        final Counter recordedPosition)
    {
        this.recordingId = recordingId;
        this.startPosition = startPosition;
        this.recordedPosition = recordedPosition;
        this.archiveDirChannel = archiveDirChannel;

        archiveDir = context.archiveDir();
        segmentFileLength = Math.max(context.segmentFileLength(), termBufferLength);
        forceWrites = context.fileSyncLevel() > 0;
        forceMetadata = context.fileSyncLevel() > 1;

        segmentIndex = segmentFileIndex(startPosition, joinPosition, segmentFileLength);
    }

    public void onBlock(
        final DirectBuffer termBuffer,
        final int termOffset,
        final int length,
        final int sessionId,
        final int termId)
    {
        try
        {
            if (segmentFileLength == segmentPosition)
            {
                onFileRollOver();
            }

            final ByteBuffer byteBuffer = termBuffer.byteBuffer();
            byteBuffer.limit(termOffset + length).position(termOffset);

            do
            {
                recordingFileChannel.write(byteBuffer);
            }
            while (byteBuffer.remaining() > 0);

            if (forceWrites)
            {
                recordingFileChannel.force(forceMetadata);
            }

            segmentPosition += length;
            recordedPosition.getAndAddOrdered(length);
        }
        catch (final ClosedByInterruptException ex)
        {
            //noinspection ResultOfMethodCallIgnored
            Thread.interrupted();
            close();
            throw new ArchiveException("file closed by interrupt, recording aborted", ex, ArchiveException.GENERIC);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public long recordingId()
    {
        return recordingId;
    }

    public long startPosition()
    {
        return startPosition;
    }

    public int segmentFileLength()
    {
        return segmentFileLength;
    }

    public void close()
    {
        if (isClosed)
        {
            return;
        }

        isClosed = true;
        CloseHelper.close(recordingFileChannel);
    }

    void init(final int segmentOffset) throws IOException
    {
        segmentPosition = segmentOffset;
        openRecordingSegmentFile();

        if (segmentOffset != 0)
        {
            recordingFileChannel.position(segmentOffset);
        }
    }

    boolean isClosed()
    {
        return isClosed;
    }

    private void openRecordingSegmentFile()
    {
        final File file = new File(archiveDir, segmentFileName(recordingId, segmentIndex));

        RandomAccessFile recordingFile = null;
        try
        {
            recordingFile = new RandomAccessFile(file, "rw");
            recordingFile.setLength(segmentFileLength);
            recordingFileChannel = recordingFile.getChannel();
            if (forceWrites && null != archiveDirChannel)
            {
                archiveDirChannel.force(forceMetadata);
            }
        }
        catch (final IOException ex)
        {
            CloseHelper.close(recordingFile);
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void onFileRollOver()
    {
        CloseHelper.close(recordingFileChannel);
        segmentPosition = 0;
        segmentIndex++;

        openRecordingSegmentFile();
    }
}

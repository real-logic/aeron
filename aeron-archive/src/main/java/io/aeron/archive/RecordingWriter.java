/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.Image;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.BlockHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;

import static io.aeron.logbuffer.FrameDescriptor.PADDING_FRAME_TYPE;
import static io.aeron.logbuffer.FrameDescriptor.typeOffset;

/**
 * Responsible for writing out a recording into the file system. A recording has descriptor file and a set of data files
 * written into the archive folder.
 * <p>
 * <b>Design note:</b> While this class is notionally closely related to the {@link RecordingSession} it is separated
 * from it for the following reasons:
 * <ul>
 * <li>Easier testing and in particular simplified re-use in testing.</li>
 * <li>Isolation of an external relationship, namely the file system.</li>
 * </ul>
 */
class RecordingWriter implements BlockHandler
{
    private final long recordingId;
    private final int segmentLength;
    private final boolean forceWrites;
    private final boolean forceMetadata;
    private final FileChannel archiveDirChannel;
    private final File archiveDir;

    private int segmentOffset;
    private int segmentIndex;
    private FileChannel recordingFileChannel;

    private boolean isClosed = false;

    RecordingWriter(
        final long recordingId,
        final long startPosition,
        final int segmentLength,
        final Image image,
        final Archive.Context ctx,
        final FileChannel archiveDirChannel)
    {
        this.recordingId = recordingId;
        this.archiveDirChannel = archiveDirChannel;
        this.segmentLength = segmentLength;

        archiveDir = ctx.archiveDir();
        forceWrites = ctx.fileSyncLevel() > 0;
        forceMetadata = ctx.fileSyncLevel() > 1;

        final long joinPosition = image.joinPosition();
        final long startTermBasePosition = startPosition - (startPosition & (image.termBufferLength() - 1));
        segmentOffset = (int)(joinPosition - startTermBasePosition) & (segmentLength - 1);
        segmentIndex = Archive.segmentFileIndex(startPosition, joinPosition, segmentLength);
    }

    public void onBlock(
        final DirectBuffer termBuffer, final int termOffset, final int length, final int sessionId, final int termId)
    {
        try
        {
            final boolean isPaddingFrame = termBuffer.getShort(typeOffset(termOffset)) == PADDING_FRAME_TYPE;
            final int dataLength = isPaddingFrame ? DataHeaderFlyweight.HEADER_LENGTH : length;
            final ByteBuffer byteBuffer = termBuffer.byteBuffer();
            byteBuffer.limit(termOffset + dataLength).position(termOffset);

            do
            {
                recordingFileChannel.write(byteBuffer);
            }
            while (byteBuffer.remaining() > 0);

            if (forceWrites)
            {
                recordingFileChannel.force(forceMetadata);
            }

            segmentOffset += length;
            if (segmentOffset >= segmentLength)
            {
                onFileRollOver();
            }
            else if (isPaddingFrame)
            {
                recordingFileChannel.position(segmentOffset);
            }
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

    void close()
    {
        if (!isClosed)
        {
            CloseHelper.close(recordingFileChannel);
            isClosed = true;
        }
    }

    void init() throws IOException
    {
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
        final File file = new File(archiveDir, Archive.segmentFileName(recordingId, segmentIndex));

        RandomAccessFile recordingFile = null;
        try
        {
            recordingFile = new RandomAccessFile(file, "rw");
            recordingFile.setLength(segmentLength);
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
        segmentOffset = 0;
        segmentIndex++;

        openRecordingSegmentFile();
    }
}

/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;

import static io.aeron.archive.client.AeronArchive.segmentFileBasePosition;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static org.agrona.BitUtil.align;
import static org.agrona.Checksums.crc32;

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
    private final boolean crcEnabled;
    private final FileChannel archiveDirChannel;
    private final File archiveDir;

    private long segmentBasePosition;
    private int segmentOffset;
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

        crcEnabled = ctx.recordingCrcEnabled();

        final int termLength = image.termBufferLength();
        final long joinPosition = image.joinPosition();
        segmentBasePosition = segmentFileBasePosition(startPosition, joinPosition, termLength, segmentLength);
        segmentOffset = (int)(joinPosition - segmentBasePosition);
    }

    public void onBlock(
        final DirectBuffer termBuffer, final int termOffset, final int length, final int sessionId, final int termId)
    {
        try
        {
            final boolean isPaddingFrame = termBuffer.getShort(typeOffset(termOffset)) == PADDING_FRAME_TYPE;
            final int dataLength = isPaddingFrame ? HEADER_LENGTH : length;
            final ByteBuffer byteBuffer = termBuffer.byteBuffer();
            byteBuffer.limit(termOffset + dataLength).position(termOffset);

            if (!isPaddingFrame && crcEnabled)
            {
                // Cast to UnsafeBuffer is safe, because BlockHandler is internal API which is always invoked with an
                // instance of UnsafeBuffer which is used for performance reasons (i.e. to avoid invokeinterface calls)
                computeCRC((UnsafeBuffer)termBuffer, termOffset, length);
            }

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
            close();
            throw new ArchiveException("file closed by interrupt, recording aborted", ex, ArchiveException.GENERIC);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void computeCRC(final UnsafeBuffer termBuffer, final int termOffset, final int length)
    {
        int frameOffset = termOffset;
        final int endOffset = termOffset + length;
        final long address = termBuffer.addressOffset();
        while (frameOffset < endOffset)
        {
            final int frameLength = frameLength(termBuffer, frameOffset);
            if (frameLength == 0)
            {
                break;
            }
            final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
            final int frameType = frameType(termBuffer, frameOffset);
            if (HDR_TYPE_DATA == frameType)
            {
                final int checksum = crc32(
                    0, address, frameOffset + HEADER_LENGTH, alignedLength - HEADER_LENGTH);
                frameSessionId(termBuffer, frameOffset, checksum);
            }
            frameOffset += alignedLength;
        }
    }

    void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            CloseHelper.quietClose(recordingFileChannel);
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
        final File file = new File(archiveDir, Archive.segmentFileName(recordingId, segmentBasePosition));

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
        segmentBasePosition += segmentLength;

        openRecordingSegmentFile();
    }
}

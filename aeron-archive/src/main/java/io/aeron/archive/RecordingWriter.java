/*
 * Copyright 2014-2025 Real Logic Limited.
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
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.client.ArchiveException;
import io.aeron.exceptions.StorageSpaceException;
import io.aeron.logbuffer.BlockHandler;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.NanoClock;
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
import static org.agrona.BitUtil.align;

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
final class RecordingWriter implements BlockHandler, AutoCloseable
{
    private final long recordingId;
    private final int segmentLength;
    private final boolean forceWrites;
    private final boolean forceMetadata;
    private final UnsafeBuffer checksumBuffer;
    private final Checksum checksum;
    private final FileChannel archiveDirChannel;
    private final File archiveDir;
    private final CountedErrorHandler countedErrorHandler;
    private final NanoClock nanoClock;
    private final Archive.Context ctx;

    private final ArchiveConductor.Recorder recorder;

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
        final ArchiveConductor.Recorder recorder)
    {
        this.recordingId = recordingId;
        this.archiveDirChannel = ctx.archiveDirChannel();
        this.segmentLength = segmentLength;

        archiveDir = ctx.archiveDir();
        forceWrites = ctx.fileSyncLevel() > 0;
        forceMetadata = ctx.fileSyncLevel() > 1;

        countedErrorHandler = ctx.countedErrorHandler();
        checksumBuffer = ctx.recordChecksumBuffer();
        checksum = ctx.recordChecksum();
        nanoClock = ctx.nanoClock();
        this.ctx = ctx;
        this.recorder = recorder;

        final int termLength = image.termBufferLength();
        final long joinPosition = image.joinPosition();
        segmentBasePosition = segmentFileBasePosition(startPosition, joinPosition, termLength, segmentLength);
        segmentOffset = (int)(joinPosition - segmentBasePosition);
    }

    /**
     * {@inheritDoc}
     */
    public void onBlock(
        final DirectBuffer termBuffer, final int termOffset, final int length, final int sessionId, final int termId)
    {
        try
        {
            final boolean isPaddingFrame = termBuffer.getShort(typeOffset(termOffset)) == PADDING_FRAME_TYPE;
            final int dataLength = isPaddingFrame ? HEADER_LENGTH : length;
            final ByteBuffer byteBuffer;

            final long startNs = nanoClock.nanoTime();
            if (null == checksum || isPaddingFrame)
            {
                byteBuffer = termBuffer.byteBuffer();
                byteBuffer.limit(termOffset + dataLength).position(termOffset);
            }
            else
            {
                checksumBuffer.putBytes(0, termBuffer, termOffset, dataLength);
                computeChecksum(checksum, checksumBuffer, dataLength);
                byteBuffer = checksumBuffer.byteBuffer();
                byteBuffer.limit(dataLength).position(0);
            }

            int fileOffset = segmentOffset;
            do
            {
                fileOffset += recordingFileChannel.write(byteBuffer, fileOffset);
            }
            while (byteBuffer.remaining() > 0);

            if (forceWrites)
            {
                recordingFileChannel.force(forceMetadata);
            }

            final long writeTimeNs = nanoClock.nanoTime() - startNs;
            recorder.bytesWritten(dataLength);
            recorder.writeTimeNs(writeTimeNs);

            segmentOffset += length;
            if (segmentOffset >= segmentLength)
            {
                onFileRollOver();
            }
        }
        catch (final ClosedByInterruptException ex)
        {
            close();
            throw new ArchiveException("file closed by interrupt, recording aborted", ex, ArchiveException.GENERIC);
        }
        catch (final IOException ex)
        {
            close();
            checkErrorType(ex, length);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            CloseHelper.close(countedErrorHandler, recordingFileChannel);
        }
    }

    long position()
    {
        return segmentBasePosition + segmentOffset;
    }

    void init() throws IOException
    {
        openRecordingSegmentFile(new File(archiveDir, Archive.segmentFileName(recordingId, segmentBasePosition)));

        if (segmentOffset != 0)
        {
            recordingFileChannel.position(segmentOffset);
        }
    }

    private void computeChecksum(final Checksum checksum, final UnsafeBuffer buffer, final int length)
    {
        final long address = buffer.addressOffset();
        int frameOffset = 0;

        while (frameOffset < length)
        {
            final int alignedLength = align(frameLength(buffer, frameOffset), FRAME_ALIGNMENT);
            final int computedChecksum = checksum.compute(
                address, frameOffset + HEADER_LENGTH, alignedLength - HEADER_LENGTH);
            frameSessionId(buffer, frameOffset, computedChecksum);
            frameOffset += alignedLength;
        }
    }

    private void openRecordingSegmentFile(final File segmentFile)
    {
        RandomAccessFile recordingFile = null;
        try
        {
            recordingFile = new RandomAccessFile(segmentFile, "rw");
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

        final File file = new File(archiveDir, Archive.segmentFileName(recordingId, segmentBasePosition));
        if (file.exists())
        {
            throw new ArchiveException("segment file already exists: " + file);
        }

        openRecordingSegmentFile(file);
    }

    private void checkErrorType(final IOException ex, final int writeLength)
    {
        boolean isLowStorageSpace = false;
        IOException suppressed = null;

        try
        {
            isLowStorageSpace = StorageSpaceException.isStorageSpaceError(ex) ||
                ctx.archiveFileStore().getUsableSpace() < writeLength;
        }
        catch (final IOException ex2)
        {
            suppressed = ex2;
        }

        final int errorCode = isLowStorageSpace ? ArchiveException.STORAGE_SPACE : ArchiveException.GENERIC;
        final ArchiveException error = new ArchiveException("java.io.IOException - " + ex.getMessage(), ex, errorCode);

        if (null != suppressed)
        {
            error.addSuppressed(suppressed);
        }

        throw error;
    }
}

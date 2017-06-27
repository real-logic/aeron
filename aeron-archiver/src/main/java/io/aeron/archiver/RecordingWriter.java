/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron.archiver;

import io.aeron.archiver.codecs.RecordingDescriptorEncoder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.RawBlockHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveUtil.recordingOffset;
import static java.nio.file.StandardOpenOption.*;

/**
 * Responsible for writing out a recording into the file system. A recording has metadata file and a set of data files
 * written into the archive folder.
 * <p>
 * Design note: While this class is notionally closely related to the {@link RecordingSession} it is separated from it
 * for the following reasons:
 * <ul>
 * <li> Easier testing and in particular simplified re-use in testing. </li>
 * <li> Isolation of an external relationship, namely the FS</li>
 * <li> While a {@link RecordingWriter} is part of a {@link RecordingSession}, a session may transition without actually
 * creating a {@link RecordingWriter}.</li>
 * </ul>
 */
class RecordingWriter implements AutoCloseable, RawBlockHandler
{
    private static final boolean VALIDATE_POSITION_ASSUMPTIONS = true;

    static class Marker
    {
        private final ByteBuffer buffer =
            BufferUtil.allocateDirectAligned(DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
        private final DataHeaderFlyweight header = new DataHeaderFlyweight();

        Marker()
        {
            header.wrap(buffer);
            header.headerType(HeaderFlyweight.HDR_TYPE_PAD);
        }
    }

    static final long NULL_TIME = -1L;
    static final long NULL_POSITION = -1;

    /* This is implicit in the file as allocated files are zeroed by reasonable OSs*/
    static final int END_OF_DATA_INDICATOR = 0;

    static final int END_OF_RECORDING_INDICATOR = -1;

    private static final int NULL_SEGMENT_POSITION = -1;
    // TODO: pull this out to the Agent and pass into the writer so no need for thread local.
    private static final ThreadLocal<Marker> MARKER = ThreadLocal.withInitial(Marker::new);

    private final boolean forceWrites;

    private final int initialTermId;
    private final int termBufferLength;
    private final int termsMask;
    private final long recordingId;

    private final File archiveDir;
    private final EpochClock epochClock;

    private final MappedByteBuffer metaDataBuffer;
    private final RecordingDescriptorEncoder metaDataEncoder;
    private final int segmentFileLength;
    private final long joinPosition;

    /**
     * Index is in the range 0:segmentFileLength, except before the first block for this image is received indicated
     * by -1
     */
    private int segmentPosition = NULL_SEGMENT_POSITION;
    private int segmentIndex = 0;
    private FileChannel recordingFileChannel;

    private boolean closed = false;
    private long endPosition = NULL_POSITION;
    private long joinTimestamp = NULL_TIME;
    private long endTimestamp = NULL_TIME;

    RecordingWriter(
        final RecordingContext recordingContext,
        final long recordingId,
        final int termBufferLength,
        final int mtuLength,
        final int initialTermId,
        final long joinPosition,
        final int sessionId,
        final int streamId,
        final String channel,
        final String sourceIdentity)
    {
        this.epochClock = recordingContext.epochClock;
        this.archiveDir = recordingContext.archiveDir;
        this.segmentFileLength = Math.max(recordingContext.segmentFileLength, termBufferLength);
        this.forceWrites = recordingContext.forceWrites;

        this.recordingId = recordingId;
        this.termBufferLength = termBufferLength;
        this.initialTermId = initialTermId;
        this.joinPosition = joinPosition;
        this.endPosition = joinPosition;

        this.termsMask = (segmentFileLength / termBufferLength) - 1;
        if (((termsMask + 1) & termsMask) != 0)
        {
            throw new IllegalArgumentException(
                "It is assumed the termBufferLength is a power of 2, and that the number of terms" +
                    "in a file is also a power of 2");
        }

        final String recordingMetaFileName = ArchiveUtil.recordingMetaFileName(recordingId);
        final File metadataFile = new File(archiveDir, recordingMetaFileName);
        try
        {
            metaDataBuffer = mapMetaData(metadataFile);
        }
        catch (final IOException ex)
        {
            close();
            throw new RuntimeException(ex);
        }

        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(metaDataBuffer);
        metaDataEncoder = new RecordingDescriptorEncoder().wrap(unsafeBuffer, Catalog.CATALOG_FRAME_LENGTH);

        initDescriptor(
            metaDataEncoder,
            recordingId,
            termBufferLength,
            segmentFileLength,
            mtuLength,
            initialTermId,
            joinPosition,
            sessionId,
            streamId,
            channel,
            sourceIdentity);

        unsafeBuffer.putInt(0, metaDataEncoder.encodedLength());
        forceMetaData(metaDataBuffer);
    }

    static void initDescriptor(
        final RecordingDescriptorEncoder recordingDescriptorEncoder,
        final long recordingId,
        final int termBufferLength,
        final int segmentFileLength,
        final int mtuLength,
        final int initialTermId,
        final long joinPosition,
        final int sessionId,
        final int streamId,
        final String channel,
        final String sourceIdentity)
    {
        recordingDescriptorEncoder
            .recordingId(recordingId)
            .termBufferLength(termBufferLength)
            .joinTimestamp(NULL_TIME)
            .joinPosition(joinPosition)
            .endPosition(joinPosition)
            .endTimestamp(NULL_TIME)
            .mtuLength(mtuLength)
            .initialTermId(initialTermId)
            .sessionId(sessionId)
            .streamId(streamId)
            .segmentFileLength(segmentFileLength)
            .channel(channel)
            .sourceIdentity(sourceIdentity);
    }

    public void onBlock(
        final FileChannel fileChannel,
        final long fileOffset,
        final UnsafeBuffer termBuffer,
        final int termOffset,
        final int blockLength,
        final int sessionId,
        final int termId)
    {
        try
        {
            if (NULL_POSITION == segmentPosition)
            {
                onFirstWrite(termOffset);
            }

            if (segmentFileLength == segmentPosition)
            {
                onFileRollOver();
            }

            validateWritePreConditions(termId, termOffset);
            // Note: since files are assumed pre-filled with 0 there's no need to write the marker ahead of the data
            final long written = transferDataFrom(fileChannel, fileOffset, blockLength);
            if (written != blockLength)
            {
                throw new IllegalStateException();
            }

            if (forceWrites)
            {
                forceData(recordingFileChannel);
            }

            afterWrite(blockLength);
            validateWritePostConditions();
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Convenience method for testing purposes only.
     */
    void writeFragment(final DirectBuffer buffer, final Header header)
    {
        final int termOffset = header.termOffset();
        final int frameLength = header.frameLength();
        final int alignedLength = BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);

        try
        {
            if (NULL_POSITION == segmentPosition)
            {
                onFirstWrite(termOffset);
            }
            if (segmentFileLength == segmentPosition)
            {
                onFileRollOver();
            }

            validateWritePreConditions(header.termId(), termOffset);
            // Note: since files are assumed pre-filled with 0 there's no need to write the marker ahead of the data
            final ByteBuffer src = buffer.byteBuffer().duplicate();
            src.position(termOffset).limit(termOffset + frameLength);
            final int written = writeData(src, segmentPosition, recordingFileChannel);
            recordingFileChannel.position(segmentPosition + alignedLength);

            if (written != frameLength)
            {
                throw new IllegalStateException();
            }

            if (forceWrites)
            {
                forceData(recordingFileChannel);
            }
            afterWrite(alignedLength);
            validateWritePostConditions();
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public void close()
    {
        if (closed)
        {
            return;
        }

        closed = true;

        if (metaDataBuffer != null)
        {
            endTimestamp = epochClock.time();
            metaDataEncoder.endTimestamp(endTimestamp);
            forceMetaData(metaDataBuffer);
            IoUtil.unmap(metaDataBuffer);
        }

        if (recordingFileChannel != null)
        {
            // write end of recording marker after data
            final Marker marker = MARKER.get();

            marker.header.frameLength(END_OF_RECORDING_INDICATOR);
            try
            {
                marker.buffer.clear();
                writeData(marker.buffer, segmentPosition, recordingFileChannel);
            }
            catch (final IOException e)
            {
                LangUtil.rethrowUnchecked(e);
            }
            finally
            {
                CloseHelper.close(recordingFileChannel);
            }
        }
    }

    long recordingId()
    {
        return recordingId;
    }

    int segmentFileLength()
    {
        return segmentFileLength;
    }

    long joinPosition()
    {
        return joinPosition;
    }

    long endPosition()
    {
        return endPosition;
    }

    long joinTimestamp()
    {
        return joinTimestamp;
    }

    long endTimestamp()
    {
        return endTimestamp;
    }

    // extend for testing
    MappedByteBuffer mapMetaData(final File file) throws IOException
    {
        try (FileChannel metadataFileChannel = FileChannel.open(file.toPath(), CREATE_NEW, READ, WRITE))
        {
            return metadataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
        }
    }

    // extend for testing
    int writeData(final ByteBuffer buffer, final int position, final FileChannel fileChannel) throws IOException
    {
        return fileChannel.write(buffer, position);
    }

    // extend for testing
    long transferDataFrom(
        final FileChannel fromFileChannel,
        final long fileOffset,
        final int blockLength) throws IOException
    {
        return fromFileChannel.transferTo(fileOffset, blockLength, recordingFileChannel);
    }

    // extend for testing
    void newRecordingSegmentFile()
    {
        final String segmentFileName = ArchiveUtil.recordingDataFileName(recordingId, segmentIndex);
        final File file = new File(archiveDir, segmentFileName);

        RandomAccessFile recordingFile = null;
        try
        {
            recordingFile = new RandomAccessFile(file, "rw");
            // Note: extra room allocated for marker write at end of file. This is required so that we can tell from
            // the data files when a recording is done.
            recordingFile.setLength(segmentFileLength + DataHeaderFlyweight.HEADER_LENGTH);
            recordingFileChannel = recordingFile.getChannel();
        }
        catch (final IOException ex)
        {
            CloseHelper.quietClose(recordingFile);
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    // extend for testing
    void forceData(final FileChannel fileChannel) throws IOException
    {
        fileChannel.force(false);
    }

    private void forceMetaData(final MappedByteBuffer metaDataBuffer)
    {
        metaDataBuffer.force();
    }

    private void onFileRollOver()
    {
        CloseHelper.close(recordingFileChannel);
        segmentPosition = 0;
        segmentIndex++;
        newRecordingSegmentFile();
    }

    private void onFirstWrite(final int termOffset) throws IOException
    {
        validateStartTermOffset(termOffset);

        segmentPosition = termOffset;
        joinTimestamp = epochClock.time();
        metaDataEncoder.joinTimestamp(joinTimestamp);
        forceMetaData(metaDataBuffer);

        newRecordingSegmentFile();

        if (segmentPosition != 0)
        {
            // padding frame required
            final Marker marker = MARKER.get();
            marker.header.frameLength(termOffset);
            marker.buffer.clear();
            writeData(marker.buffer, 0, recordingFileChannel);
            // we need to setup starting position for transferTo
            recordingFileChannel.position(segmentPosition);
        }
    }

    private void afterWrite(final int blockLength)
    {
        segmentPosition += blockLength;
        endPosition += blockLength;
        metaDataEncoder.endPosition(endPosition);
    }

    private void validateStartTermOffset(final int termOffset)
    {
        if (VALIDATE_POSITION_ASSUMPTIONS)
        {
            final int expectedStartTermOffset = (int)(joinPosition & (termBufferLength - 1));
            if (expectedStartTermOffset != termOffset)
            {
                throw new IllegalStateException();
            }
        }
    }

    private void validateWritePreConditions(final int termId, final int termOffset) throws IOException
    {
        if (VALIDATE_POSITION_ASSUMPTIONS)
        {
            if (recordingFileChannel.position() != segmentPosition)
            {
                throw new IllegalStateException();
            }

            final int recordingOffset = recordingOffset(
                termOffset,
                termId,
                initialTermId,
                termsMask,
                termBufferLength);

            if (recordingOffset != segmentPosition)
            {
                throw new IllegalStateException();
            }
        }
    }

    private void validateWritePostConditions() throws IOException
    {
        if (VALIDATE_POSITION_ASSUMPTIONS)
        {
            if (recordingFileChannel.position() != segmentPosition)
            {
                throw new IllegalStateException();
            }
        }
    }

    static class RecordingContext
    {
        private File archiveDir;
        private EpochClock epochClock;
        private boolean forceWrites = true;
        private int segmentFileLength = 1024 * 1024 * 1024;

        RecordingContext archiveDir(final File archiveDir)
        {
            this.archiveDir = archiveDir;
            return this;
        }

        RecordingContext epochClock(final EpochClock epochClock)
        {
            this.epochClock = epochClock;
            return this;
        }

        RecordingContext forceWrites(final boolean forceWrites)
        {
            this.forceWrites = forceWrites;
            return this;
        }

        RecordingContext recordingFileLength(final int recordingFileLength)
        {
            this.segmentFileLength = recordingFileLength;
            return this;
        }

        int recordingFileLength()
        {
            return segmentFileLength;
        }
    }
}

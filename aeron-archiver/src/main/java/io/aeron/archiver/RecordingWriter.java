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
import io.aeron.logbuffer.*;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveUtil.recordingOffset;
import static java.nio.file.StandardOpenOption.*;

/**
 * Responsible for writing out a recording into the file system. A recording has metdata file and a set of data files
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
final class RecordingWriter implements AutoCloseable, RawBlockHandler
{
    static class Marker
    {
        private final ByteBuffer buffer =
            BufferUtil.allocateDirectAligned(DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
        private final DataHeaderFlyweight header = new DataHeaderFlyweight();

        Marker()
        {
            header.wrap(buffer);
            header.headerType(HeaderFlyweight.HDR_TYPE_PAD);
            header.flags(FrameDescriptor.UNFRAGMENTED);
        }
    }

    static final long NULL_TIME = -1L;
    static final long NULL_POSITION = -1;
    static final int END_OF_RECORDING_INDICATOR = -1;
    static final int END_OF_DATA_INDICATOR = 0;

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
    private boolean stopped = false;
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
        this.segmentFileLength = recordingContext.segmentFileLength;
        this.forceWrites = recordingContext.forceWrites;

        this.recordingId = recordingId;
        this.termBufferLength = termBufferLength;
        this.initialTermId = initialTermId;
        this.joinPosition = joinPosition;

        this.termsMask = (segmentFileLength / termBufferLength) - 1;
        if (((termsMask + 1) & termsMask) != 0)
        {
            throw new IllegalArgumentException(
                "It is assumed the termBufferLength is a power of 2, and that the number of terms" +
                    "in a file is also a power of 2");
        }

        final String recordingMetaFileName = ArchiveUtil.recordingMetaFileName(recordingId);
        final File file = new File(archiveDir, recordingMetaFileName);
        try (FileChannel metadataFileChannel = FileChannel.open(file.toPath(), CREATE_NEW, READ, WRITE))
        {
            metaDataBuffer = metadataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
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
        }
        catch (final IOException ex)
        {
            close();

            throw new RuntimeException(ex);
        }
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
            .endPosition(NULL_POSITION)
            .endTimestamp(NULL_TIME)
            .mtuLength(mtuLength)
            .initialTermId(initialTermId)
            .sessionId(sessionId)
            .streamId(streamId)
            .segmentFileLength(segmentFileLength)
            .channel(channel)
            .sourceIdentity(sourceIdentity);
    }

    private void newRecordingSegmentFile()
    {
        final String segmentFileName = ArchiveUtil.recordingDataFileName(recordingId, segmentIndex);
        final File file = new File(archiveDir, segmentFileName);

        try
        {
            final RandomAccessFile recordingFile = new RandomAccessFile(file, "rw");
            recordingFile.setLength(segmentFileLength + DataHeaderFlyweight.HEADER_LENGTH);
            recordingFileChannel = recordingFile.getChannel();
        }
        catch (final IOException ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }
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
        final int recordingOffset = recordingOffset(termOffset, termId, initialTermId, termsMask, termBufferLength);
        try
        {
            prepareRecording(termOffset, blockLength);

            fileChannel.transferTo(fileOffset, blockLength, recordingFileChannel);
            if (forceWrites)
            {
                recordingFileChannel.force(false);
            }

            writePrologue(termOffset, blockLength, termId, recordingOffset);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Convenience method for testing purposes.
     */
    void writeFragment(final DirectBuffer buffer, final Header header)
    {
        final int termId = header.termId();
        final int termOffset = header.termOffset();
        final int frameLength = header.frameLength();
        final int alignedLength = BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);

        final int recordingOffset = recordingOffset(termOffset, termId, initialTermId, termsMask, termBufferLength);
        try
        {
            prepareRecording(termOffset, alignedLength);

            final ByteBuffer src = buffer.byteBuffer().duplicate();
            src.position(termOffset).limit(termOffset + frameLength);
            recordingFileChannel.write(src);

            writePrologue(termOffset, alignedLength, termId, recordingOffset);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void prepareRecording(final int termOffset, final int length) throws IOException
    {
        final Marker marker = MARKER.get();

        if (segmentPosition == -1)
        {
            newRecordingSegmentFile();

            segmentPosition = termOffset;
            marker.header.frameLength(termOffset);
            marker.buffer.clear();
            recordingFileChannel.write(marker.buffer, 0);
            recordingFileChannel.position(segmentPosition);
            joinTimestamp = epochClock.time();
            metaDataEncoder.joinTimestamp(joinTimestamp);
        }
        // write ahead of data to indicate end of data in file
        marker.header.frameLength(END_OF_DATA_INDICATOR);
        marker.buffer.clear();
        recordingFileChannel.write(marker.buffer, segmentPosition + length);
    }

    private void writePrologue(
        final int termOffset,
        final int blockLength,
        final int termId,
        final int recordingOffset)
        throws IOException
    {
        segmentPosition = recordingOffset + blockLength;
        final int resultingOffset = termOffset + blockLength;
        final long position = ((long)(termId - initialTermId)) * termBufferLength + resultingOffset;
        metaDataEncoder.endPosition(position);
        endPosition = position;

        if (segmentPosition == segmentFileLength)
        {
            CloseHelper.close(recordingFileChannel);
            segmentPosition = 0;
            segmentIndex++;
            newRecordingSegmentFile();
        }
    }

    void stop()
    {
        endTimestamp = epochClock.time();
        metaDataEncoder.endTimestamp(endTimestamp);
        metaDataBuffer.force();
        stopped = true;
    }

    public void close()
    {
        if (closed)
        {
            return;
        }
        // write ahead of data to indicate end of recording
        if (recordingFileChannel != null)
        {
            final Marker marker = MARKER.get();

            marker.header.frameLength(END_OF_RECORDING_INDICATOR);
            try
            {
                marker.buffer.clear();
                recordingFileChannel.write(marker.buffer, segmentPosition);
            }
            catch (final IOException e)
            {
                // TODO: ???
                e.printStackTrace();
            }
        }

        CloseHelper.close(recordingFileChannel);

        if (metaDataBuffer != null && !stopped)
        {
            stop();
        }

        IoUtil.unmap(metaDataBuffer);

        closed = true;
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

    static class RecordingContext
    {
        private File archiveDir;
        private EpochClock epochClock;
        private boolean forceWrites = true;
        private int segmentFileLength = 128 * 1024 * 1024;

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

        RecordingContext recordingFileLength(final int recordingFileSize)
        {
            this.segmentFileLength = recordingFileSize;
            return this;
        }

        int recordingFileLength()
        {
            return segmentFileLength;
        }
    }
}

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
import io.aeron.protocol.*;
import org.agrona.*;
import org.agrona.concurrent.*;

import java.io.*;
import java.nio.*;
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
    static final long NULL_TIME = -1L;
    static final long NULL_POSITION = -1;
    private static final int NULL_SEGMENT_POSITION = -1;

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

    /**
     * Index is in the range 0:segmentFileLength, except before the first block for this image is received indicated
     * by -1
     */
    private int segmentPosition = NULL_SEGMENT_POSITION;
    private int segmentIndex = 0;
    private RandomAccessFile recordingFile;
    private FileChannel recordingFileChannel;

    private boolean closed = false;
    private boolean stopped = false;
    private long joiningPosition;
    private long lastPosition;

    RecordingWriter(
        final RecordingContext recordingContext,
        final long recordingId,
        final int termBufferLength,
        final int mtuLength,
        final int initialTermId,
        final long joiningPosition,
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
        this.joiningPosition = joiningPosition;

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
            // TODO: Do not create too many files and mappings. Meta data should only be in the catalog.
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
                joiningPosition,
                sessionId,
                streamId,
                channel,
                sourceIdentity);

            unsafeBuffer.putInt(0, metaDataEncoder.encodedLength());
        }
        catch (final IOException ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
            // the next line is to keep compiler happy with regards to final fields init
            throw new RuntimeException();
        }
    }

    static void initDescriptor(
        final RecordingDescriptorEncoder recordingDescriptorEncoder,
        final long recordingId,
        final int termBufferLength,
        final int segmentFileLength,
        final int mtuLength,
        final int initialTermId,
        final long joiningPosition,
        final int sessionId,
        final int streamId,
        final String channel,
        final String sourceIdentity)
    {
        recordingDescriptorEncoder
            .recordingId(recordingId)
            .termBufferLength(termBufferLength)
            .startTime(NULL_TIME)
            .joiningPosition(joiningPosition)
            .lastPosition(NULL_POSITION)
            .endTime(NULL_TIME)
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
            recordingFile = new RandomAccessFile(file, "rw");
            recordingFile.setLength(segmentFileLength);
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
            prepareRecording(termOffset);

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

        final int recordingOffset = recordingOffset(termOffset, termId, initialTermId, termsMask, termBufferLength);
        try
        {
            prepareRecording(termOffset);

            final ByteBuffer src = buffer.byteBuffer().duplicate();
            src.position(termOffset).limit(termOffset + frameLength);
            recordingFileChannel.write(src);

            writePrologue(termOffset, frameLength, termId, recordingOffset);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void prepareRecording(final int termOffset) throws IOException
    {
        if (segmentPosition == -1)
        {
            newRecordingSegmentFile();

            segmentPosition = termOffset;
            if (termOffset != 0)
            {
                final ByteBuffer padBuffer = ByteBuffer.allocate(DataHeaderFlyweight.HEADER_LENGTH);
                final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
                headerFlyweight.wrap(padBuffer);
                headerFlyweight.headerType(HeaderFlyweight.HDR_TYPE_PAD);
                headerFlyweight.flags(FrameDescriptor.UNFRAGMENTED);
                headerFlyweight.frameLength(termOffset);
                recordingFileChannel.write(padBuffer, 0);
            }
            recordingFileChannel.position(segmentPosition);
            metaDataEncoder.startTime(epochClock.time());
        }
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
        metaDataEncoder.lastPosition(position);
        lastPosition = position;

        if (segmentPosition == segmentFileLength)
        {
            CloseHelper.close(recordingFileChannel);
            CloseHelper.close(recordingFile);
            segmentPosition = 0;
            segmentIndex++;
            newRecordingSegmentFile();
        }
    }

    void stop()
    {
        metaDataEncoder.endTime(epochClock.time());
        metaDataBuffer.force();
        stopped = true;
    }

    public void close()
    {
        if (closed)
        {
            return;
        }

        CloseHelper.close(recordingFileChannel);
        CloseHelper.close(recordingFile);

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

    ByteBuffer metaDataBuffer()
    {
        return metaDataBuffer;
    }

    int segmentFileLength()
    {
        return segmentFileLength;
    }

    long joiningPosition()
    {
        return joiningPosition;
    }

    long lastPosition()
    {
        return lastPosition;
    }

    static class RecordingContext
    {
        private File archiveDir;
        private EpochClock epochClock;
        private boolean forceWrites = true;
        private int segmentFileLength = 128 * 1024 * 1024; // TODO: make this configurable and discoverable.

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

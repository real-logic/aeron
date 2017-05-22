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
import org.agrona.*;
import org.agrona.concurrent.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveUtil.recordingOffset;
import static java.nio.file.StandardOpenOption.*;

// TODO: Should this class not be folded into RecordingSession? Why the separation and indirection?
final class Recorder implements AutoCloseable, RawBlockHandler
{
    private final boolean forceWrites;
    private final boolean forceMetadataUpdates;

    private final int initialTermId;
    private final int termBufferLength;
    private final int termsMask;
    private final long recordingId;

    private final File archiveDir;
    private final EpochClock epochClock;

    private final FileChannel metadataFileChannel;
    private final MappedByteBuffer metaDataBuffer;
    private final RecordingDescriptorEncoder metaDataEncoder;
    private final int segmentFileLength;

    /**
     * Index is in the range 0:segmentFileLength, except before the first block for this image is received indicated
     * by -1
     */
    private int segmentPosition = -1;
    private int segmentIndex = 0;
    private RandomAccessFile recordingFile;
    private FileChannel recordingFileChannel;

    private boolean closed = false;
    private boolean stopped = false;
    private long joiningPosition;
    private long lastPosition;

    private Recorder(final Builder builder)
    {
        this.recordingId = builder.recordingId;
        this.archiveDir = builder.archiveDir;
        this.termBufferLength = builder.termBufferLength;
        this.epochClock = builder.epochClock;
        this.segmentFileLength = builder.segmentFileLength;
        this.initialTermId = builder.initialTermId;
        this.termsMask = (builder.segmentFileLength / termBufferLength) - 1;
        this.forceWrites = builder.forceWrites;
        this.forceMetadataUpdates = builder.forceMetadataUpdates;
        this.joiningPosition = builder.joiningPosition;

        if (((termsMask + 1) & termsMask) != 0)
        {
            throw new IllegalArgumentException(
                "It is assumed the termBufferLength is a power of 2, and that the number of terms" +
                    "in a file is also a power of 2");
        }

        final String recordingMetaFileName = ArchiveUtil.recordingMetaFileName(recordingId);
        final File file = new File(archiveDir, recordingMetaFileName);
        try
        {
            metadataFileChannel = FileChannel.open(file.toPath(), CREATE_NEW, READ, WRITE);
            metaDataBuffer = metadataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(metaDataBuffer);
            metaDataEncoder = new RecordingDescriptorEncoder().wrap(unsafeBuffer, Catalog.CATALOG_FRAME_LENGTH);

            initDescriptor(
                metaDataEncoder,
                recordingId,
                termBufferLength,
                segmentFileLength,
                builder.mtuLength,
                builder.initialTermId,
                builder.joiningPosition,
                builder.source,
                builder.sessionId,
                builder.channel,
                builder.streamId);

            unsafeBuffer.putInt(0, metaDataEncoder.encodedLength());
            metaDataBuffer.force();
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
        final String source,
        final int sessionId,
        final String channel,
        final int streamId)
    {
        recordingDescriptorEncoder
            .recordingId(recordingId)
            .termBufferLength(termBufferLength)
            .startTime(-1)
            .joiningPosition(joiningPosition)
            .lastPosition(-1)
            .endTime(-1)
            .mtuLength(mtuLength)
            .initialTermId(initialTermId)
            .sessionId(sessionId)
            .streamId(streamId)
            .segmentFileLength(segmentFileLength)
            .source(source)
            .channel(channel);
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
            prepareRecording(termOffset, recordingOffset, blockLength);

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

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final int termId = header.termId();
        final int termOffset = header.termOffset();
        final int frameLength = header.frameLength();

        final int recordingOffset = recordingOffset(termOffset, termId, initialTermId, termsMask, termBufferLength);
        try
        {
            prepareRecording(termOffset, recordingOffset, header.frameLength());

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

    private void prepareRecording(
        final int termOffset,
        final int recordingOffset,
        final int writeLength)
        throws IOException
    {
        if (segmentPosition == -1)
        {
            newRecordingSegmentFile();

            segmentPosition = termOffset;
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
        final int endTermOffset = termOffset + blockLength;
        final long position = ((long)(termId - initialTermId)) * termBufferLength + endTermOffset;
        metaDataEncoder.lastPosition(position);
        lastPosition = position;
        if (forceMetadataUpdates)
        {
            metaDataBuffer.force();
        }

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
        CloseHelper.close(metadataFileChannel);

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

    static class Builder
    {
        private File archiveDir;
        private EpochClock epochClock;
        private long recordingId;
        private int termBufferLength;
        private boolean forceWrites = true;
        private boolean forceMetadataUpdates = true;
        private int sessionId;
        private int streamId;
        private String source;
        private String channel;
        private int segmentFileLength = 128 * 1024 * 1024;
        private int initialTermId;
        private int mtuLength;
        private long joiningPosition;

        Builder archiveDir(final File archiveDir)
        {
            this.archiveDir = archiveDir;
            return this;
        }

        Builder epochClock(final EpochClock epochClock)
        {
            this.epochClock = epochClock;
            return this;
        }

        Builder recordingId(final long recordingId)
        {
            this.recordingId = recordingId;
            return this;
        }

        Builder termBufferLength(final int termBufferLength)
        {
            this.termBufferLength = termBufferLength;
            return this;
        }

        Builder forceWrites(final boolean forceWrites)
        {
            this.forceWrites = forceWrites;
            return this;
        }

        Builder forceMetadataUpdates(final boolean forceMetadataUpdates)
        {
            this.forceMetadataUpdates = forceMetadataUpdates;
            return this;
        }

        Builder sessionId(final int sessionId)
        {
            this.sessionId = sessionId;
            return this;
        }

        Builder streamId(final int streamId)
        {
            this.streamId = streamId;
            return this;
        }

        Builder source(final String source)
        {
            this.source = source;
            return this;
        }

        Builder channel(final String channel)
        {
            this.channel = channel;
            return this;
        }

        Builder recordingFileLength(final int recordingFileSize)
        {
            this.segmentFileLength = recordingFileSize;
            return this;
        }

        Builder initialTermId(final int initialTermId)
        {
            this.initialTermId = initialTermId;
            return this;
        }

        Recorder build()
        {
            return new Recorder(this);
        }

        int recordingFileLength()
        {
            return segmentFileLength;
        }

        Builder mtuLength(final int mtuLength)
        {
            this.mtuLength = mtuLength;
            return this;
        }

        long joiningPosition()
        {
            return joiningPosition;
        }

        Builder joiningPosition(final long joiningPosition)
        {
            this.joiningPosition = joiningPosition;
            return this;
        }
    }
}

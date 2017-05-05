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

import io.aeron.archiver.codecs.RecordingDescriptorEncoder;
import io.aeron.logbuffer.*;
import org.agrona.*;
import org.agrona.concurrent.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveUtil.recordingOffset;
import static java.nio.file.StandardOpenOption.*;

final class ImageRecorder implements AutoCloseable, FragmentHandler, RawBlockHandler
{
    static class Builder
    {
        private File archiveDir;
        private EpochClock epochClock;
        private int recordingId;
        private int termBufferLength;
        private int imageInitialTermId;
        private boolean forceWrites = true;
        private boolean forceMetadataUpdates = true;
        private int sessionId;
        private int streamId;
        private String source;
        private String channel;
        private int segmentFileLength = 128 * 1024 * 1024;

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

        Builder recordingId(final int recordingId)
        {
            this.recordingId = recordingId;
            return this;
        }

        Builder termBufferLength(final int termBufferLength)
        {
            this.termBufferLength = termBufferLength;
            return this;
        }

        Builder imageInitialTermId(final int imageInitialTermId)
        {
            this.imageInitialTermId = imageInitialTermId;
            return this;
        }

        // TODO: Forcing of writes should have the option of SYNC or DSYNC so best via channel rather than mapped buffer
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

        ImageRecorder build()
        {
            return new ImageRecorder(this);
        }

        int recordingFileLength()
        {
            return segmentFileLength;
        }
    }

    private final boolean forceWrites;
    private final boolean forceMetadataUpdates;

    private final int termBufferLength;
    private final int termsMask;
    private final int recordingId;

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
    private int recordingPosition = -1;
    private RandomAccessFile recordingFile;
    private FileChannel recordingFileChannel;

    private int initialTermId = -1;
    private int initialTermOffset = -1;
    private int lastTermId = -1;
    private int lastTermOffset = -1;

    private boolean closed = false;
    private boolean stopped = false;

    private ImageRecorder(final Builder builder)
    {
        this.recordingId = builder.recordingId;
        this.archiveDir = builder.archiveDir;
        this.termBufferLength = builder.termBufferLength;
        this.epochClock = builder.epochClock;
        this.segmentFileLength = builder.segmentFileLength;

        this.termsMask = (builder.segmentFileLength / termBufferLength) - 1;
        this.forceWrites = builder.forceWrites;
        this.forceMetadataUpdates = builder.forceMetadataUpdates;
        if (((termsMask + 1) & termsMask) != 0)
        {
            throw new IllegalArgumentException(
                "It is assumed the termBufferLength is a power of 2 <= 1GB and that" +
                    "therefore the number of terms in a file is also a power of 2");
        }

        final String archiveMetaFileName = ArchiveUtil.recordingMetaFileName(recordingId);
        final File file = new File(archiveDir, archiveMetaFileName);
        try
        {
            metadataFileChannel = FileChannel.open(file.toPath(), CREATE_NEW, READ, WRITE);
            metaDataBuffer = metadataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(metaDataBuffer);
            metaDataEncoder = new RecordingDescriptorEncoder()
                .wrap(unsafeBuffer, Catalog.CATALOG_FRAME_LENGTH);

            initDescriptor(
                metaDataEncoder,
                recordingId,
                termBufferLength,
                segmentFileLength,
                builder.imageInitialTermId,
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
        final RecordingDescriptorEncoder descriptor,
        final int recordingId,
        final int termBufferLength,
        final int segmentFileLength,
        final int imageInitialTermId,
        final String source,
        final int sessionId,
        final String channel,
        final int streamId)
    {
        descriptor.recordingId(recordingId);
        descriptor.termBufferLength(termBufferLength);
        descriptor.startTime(-1);
        descriptor.initialTermId(-1);
        descriptor.initialTermOffset(-1);
        descriptor.lastTermId(-1);
        descriptor.lastTermOffset(-1);
        descriptor.endTime(-1);
        descriptor.imageInitialTermId(imageInitialTermId);
        descriptor.sessionId(sessionId);
        descriptor.streamId(streamId);
        descriptor.segmentFileLength(segmentFileLength);
        descriptor.source(source);
        descriptor.channel(channel);
    }

    private void newRecordingSegmentFile(final int termId)
    {
        final String segmentFileName = ArchiveUtil.recordingDataFileName(
            recordingId, initialTermId, termBufferLength, termId, segmentFileLength);
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
        // detect first write
        if (recordingPosition == -1 && termId != initialTermId)
        {
            // archiving an ongoing publication
            metaDataEncoder.initialTermId(termId);
            initialTermId = termId;
        }
        // TODO: if assumptions below are valid the computation is redundant for all but the first time this
        // TODO: ...method is called
        final int archiveOffset = recordingOffset(termOffset, termId, initialTermId, termsMask, termBufferLength);
        try
        {
            prepareRecording(termOffset, termId, archiveOffset, blockLength);

            fileChannel.transferTo(fileOffset, blockLength, recordingFileChannel);
            if (forceWrites)
            {
                recordingFileChannel.force(false);
            }

            writePrologue(termOffset, blockLength, termId, archiveOffset);
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

        // detect first write
        if (recordingPosition == -1 && termId != initialTermId)
        {
            // archiving an ongoing publication
            metaDataEncoder.initialTermId(termId);
            initialTermId = termId;
        }
        // TODO: if assumptions below are valid the computation is redundant for all but the first time this
        // TODO: ...method is called
        final int archiveOffset = recordingOffset(termOffset, termId, initialTermId, termsMask, termBufferLength);

        try
        {
            prepareRecording(termOffset, termId, archiveOffset, header.frameLength());

            final ByteBuffer src = buffer.byteBuffer().duplicate();
            src.position(termOffset).limit(termOffset + frameLength);
            recordingFileChannel.write(src);

            writePrologue(termOffset, frameLength, termId, archiveOffset);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void prepareRecording(
        final int termOffset,
        final int termId,
        final int recordingOffset,
        final int writeLength)
        throws IOException
    {
        if (termOffset + writeLength > termBufferLength)
        {
            throw new IllegalArgumentException("Recording across terms is not supported:" +
                " [offset=" + termOffset + " + length=" + writeLength + "] > termBufferLength=" + termBufferLength);
        }

        if (recordingPosition == -1)
        {
            newRecordingSegmentFile(termId);
            if (recordingFileChannel.position() != 0)
            {
                throw new IllegalArgumentException(
                    "It is assumed that recordingFileChannel.position() is 0 on first write");
            }

            recordingPosition = termOffset;
            // first write to the logs is not at beginning of file. We need to insert a padding indicator.
            if (recordingOffset != 0)
            {
                // would be nice to use the log buffer header for this, but actually makes no difference.
                final ByteBuffer bb = ByteBuffer.allocate(128);
                bb.putInt(0);
                bb.putInt(termOffset);
                recordingFileChannel.write(bb);
            }

            metaDataEncoder.initialTermOffset(termOffset);
            initialTermOffset = termOffset;
            recordingFileChannel.position(recordingPosition);
            metaDataEncoder.startTime(epochClock.time());
        }
        else if (recordingOffset != recordingPosition)
        {
            throw new IllegalArgumentException(
                "It is assumed that recordingPosition tracks the calculated recordingOffset");
        }
        else if (recordingFileChannel.position() != recordingPosition)
        {
            throw new IllegalArgumentException("It is assumed that recordingPosition tracks the file position");
        }
    }

    private void writePrologue(
        final int termOffset,
        final int blockLength,
        final int termId,
        final int recordingOffset)
        throws IOException
    {
        recordingPosition = recordingOffset + blockLength;
        metaDataEncoder.lastTermId(termId);
        lastTermId = termId;
        final int endTermOffset = termOffset + blockLength;
        metaDataEncoder.lastTermOffset(endTermOffset);
        lastTermOffset = endTermOffset;
        if (forceMetadataUpdates)
        {
            metaDataBuffer.force();
        }

        if (recordingPosition == segmentFileLength)
        {
            CloseHelper.close(recordingFileChannel);
            CloseHelper.close(recordingFile);
            recordingPosition = 0;
            // TODO: allocate ahead files, will also give early indication to low storage
            newRecordingSegmentFile(termId + 1);
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

    int recordingId()
    {
        return recordingId;
    }

    int initialTermId()
    {
        return initialTermId;
    }

    int initialTermOffset()
    {
        return initialTermOffset;
    }

    int lastTermId()
    {
        return lastTermId;
    }

    int lastTermOffset()
    {
        return lastTermOffset;
    }

    ByteBuffer metaDataBuffer()
    {
        return metaDataBuffer;
    }

    int segmentFileLength()
    {
        return segmentFileLength;
    }
}

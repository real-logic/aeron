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

import io.aeron.archiver.messages.ArchiveDescriptorEncoder;
import io.aeron.logbuffer.*;
import org.agrona.*;
import org.agrona.concurrent.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveFileUtil.archiveDataFileName;
import static io.aeron.archiver.ArchiveFileUtil.archiveOffset;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class ArchiveStreamWriter implements AutoCloseable, FragmentHandler, RawBlockHandler
{
    private final int imageInitialTermId;
    private final int termBufferLength;
    private final int termsMask;
    private final int streamInstanceId;

    private final File archiveFolder;
    private final FileChannel metadataFileChannel;
    private final MappedByteBuffer metaDataBuffer;
    private final ArchiveDescriptorEncoder metaDataWriter;

    private final EpochClock epochClock;
    /**
     * Index is in the range 0:ARCHIVE_FILE_SIZE, except before the first block for this image is received indicated
     * by -1
     */
    private int archivePosition = -1;
    private RandomAccessFile archiveFile;
    private FileChannel archiveFileChannel;

    private int initialTermId = -1;
    private int initialTermOffset = -1;
    private int lastTermId = -1;
    private int lastTermOffset = -1;

    private boolean closed = false;
    private boolean stopped = false;

    ArchiveStreamWriter(
        final File archiveFolder,
        final EpochClock epochClock,
        final int streamInstanceId,
        final int termBufferLength,
        final int imageInitialTermId,
        final StreamKey streamKey)
    {
        this.streamInstanceId = streamInstanceId;
        this.archiveFolder = archiveFolder;
        this.termBufferLength = termBufferLength;
        this.epochClock = epochClock;
        this.imageInitialTermId = imageInitialTermId;

        this.termsMask = (ArchiveFileUtil.ARCHIVE_FILE_SIZE / termBufferLength) - 1;
        if (((termsMask + 1) & termsMask) != 0)
        {
            throw new IllegalArgumentException(
                "It is assumed the termBufferLength is a power of 2 <= 1GB and that" +
                    "therefore the number of terms in a file is also a power of 2");
        }

        final String archiveMetaFileName = ArchiveFileUtil.archiveMetaFileName(streamInstanceId);
        final File file = new File(archiveFolder, archiveMetaFileName);
        try
        {
            metadataFileChannel = FileChannel.open(file.toPath(), CREATE_NEW, READ, WRITE);
            metaDataBuffer = metadataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(metaDataBuffer);
            metaDataWriter = new ArchiveDescriptorEncoder().wrap(unsafeBuffer, ArchiveIndex.INDEX_FRAME_LENGTH);

            initDescriptor(metaDataWriter, streamInstanceId, termBufferLength, imageInitialTermId, streamKey);

            unsafeBuffer.putInt(0, metaDataWriter.encodedLength());
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
        final ArchiveDescriptorEncoder descriptor,
        final int streamInstanceId,
        final int termBufferLength,
        final int imageInitialTermId,
        final StreamKey streamKey)
    {
        descriptor.streamInstanceId(streamInstanceId);
        descriptor.termBufferLength(termBufferLength);
        descriptor.startTime(-1);
        descriptor.initialTermId(-1);
        descriptor.initialTermOffset(-1);
        descriptor.lastTermId(-1);
        descriptor.lastTermOffset(-1);
        descriptor.endTime(-1);
        descriptor.imageInitialTermId(imageInitialTermId);
        descriptor.sessionId(streamKey.sessionId());
        descriptor.streamId(streamKey.streamId());
        descriptor.source(streamKey.source());
        descriptor.channel(streamKey.channel());
    }

    private void newArchiveFile(final int termId)
    {
        final String archiveDataFileName = archiveDataFileName(
            streamInstanceId, initialTermId, termBufferLength, termId);
        final File file = new File(archiveFolder, archiveDataFileName);

        try
        {
            archiveFile = new RandomAccessFile(file, "rwd");
            archiveFile.setLength(ArchiveFileUtil.ARCHIVE_FILE_SIZE);
            archiveFileChannel = archiveFile.getChannel();
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
        if (archivePosition == -1 && termId != initialTermId)
        {
            // archiving an ongoing publication
            metaDataWriter.initialTermId(termId);
            initialTermId = termId;
        }
        // TODO: if assumptions below are valid the computation is redundant for all but the first time this
        // TODO: ...method is called
        final int archiveOffset = archiveOffset(termOffset, termId, initialTermId, termsMask, termBufferLength);
        try
        {
            prepareWrite(termOffset, termId, archiveOffset, blockLength);

            fileChannel.transferTo(fileOffset, blockLength, archiveFileChannel);

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
        if (archivePosition == -1 && termId != initialTermId)
        {
            // archiving an ongoing publication
            metaDataWriter.initialTermId(termId);
            initialTermId = termId;
        }
        // TODO: if assumptions below are valid the computation is redundant for all but the first time this
        // TODO: ...method is called
        final int archiveOffset = archiveOffset(termOffset, termId, initialTermId, termsMask, termBufferLength);

        try
        {
            prepareWrite(termOffset, termId, archiveOffset, header.frameLength());

            final ByteBuffer src = buffer.byteBuffer().duplicate();
            src.position(termOffset).limit(termOffset + frameLength);
            archiveFileChannel.write(src);

            writePrologue(termOffset, frameLength, termId, archiveOffset);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void prepareWrite(
        final int termOffset,
        final int termId,
        final int archiveOffset,
        final int writeLength)
        throws IOException
    {
        if (termOffset + writeLength > termBufferLength)
        {
            throw new IllegalArgumentException("Writing across terms is not supported:" +
                " [offset=" + termOffset + " + length=" + writeLength + "] > termBufferLength=" + termBufferLength);
        }

        if (archivePosition == -1)
        {
            newArchiveFile(termId);
            if (archiveFileChannel.position() != 0)
            {
                throw new IllegalArgumentException(
                    "It is assumed that archiveFileChannel.position() is 0 on first write");
            }

            archivePosition = termOffset;
            // first write to the logs is not at beginning of file. We need to insert a padding indicator.
            if (archiveOffset != 0)
            {
                // would be nice to use the log buffer header for this, but actually makes no difference.
                final ByteBuffer bb = ByteBuffer.allocate(128);
                bb.putInt(0);
                bb.putInt(termOffset);
                archiveFileChannel.write(bb);
            }

            metaDataWriter.initialTermOffset(termOffset);
            initialTermOffset = termOffset;
            archiveFileChannel.position(archivePosition);
            metaDataWriter.startTime(epochClock.time());
        }
        else if (archiveOffset != archivePosition)
        {
            throw new IllegalArgumentException(
                "It is assumed that archivePosition tracks the calculated archiveOffset");
        }
        else if (archiveFileChannel.position() != archivePosition)
        {
            throw new IllegalArgumentException("It is assumed that archivePosition tracks the file position");
        }
    }

    private void writePrologue(
        final int termOffset,
        final int blockLength,
        final int termId,
        final int archiveOffset)
        throws IOException
    {
        archivePosition = archiveOffset + blockLength;
        metaDataWriter.lastTermId(termId);
        lastTermId = termId;
        final int endTermOffset = termOffset + blockLength;
        metaDataWriter.lastTermOffset(endTermOffset);
        lastTermOffset = endTermOffset;
        metaDataBuffer.force();

        if (archivePosition == ArchiveFileUtil.ARCHIVE_FILE_SIZE)
        {
            CloseHelper.quietClose(archiveFileChannel);
            CloseHelper.quietClose(archiveFile);
            archivePosition = 0;
            // TODO: allocate ahead files, will also give early indication to low storage
            newArchiveFile(termId + 1);
        }
    }

    void stop()
    {
        metaDataWriter.endTime(epochClock.time());
        metaDataBuffer.force();
        stopped = true;
    }

    public void close()
    {
        if (closed)
        {
            return;
        }

        CloseHelper.quietClose(archiveFileChannel);
        CloseHelper.quietClose(archiveFile);

        if (metaDataBuffer != null && !stopped)
        {
            stop();
        }

        IoUtil.unmap(metaDataBuffer);
        CloseHelper.quietClose(metadataFileChannel);

        closed = true;
    }

    int streamInstanceId()
    {
        return streamInstanceId;
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
}

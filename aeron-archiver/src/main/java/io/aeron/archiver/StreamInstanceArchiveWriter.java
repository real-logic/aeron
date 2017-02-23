/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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

import io.aeron.archiver.messages.ArchiveMetaFileFormatEncoder;
import io.aeron.logbuffer.*;
import org.agrona.*;
import org.agrona.concurrent.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveFileUtil.archiveDataFileName;
import static io.aeron.archiver.ArchiveFileUtil.archiveOffset;

public class StreamInstanceArchiveWriter implements AutoCloseable, FragmentHandler, RawBlockHandler
{
    private final int termBufferLength;
    private final int termsMask;
    private final int streamInstanceId;

    private final File archiveFolder;
    private final FileChannel metadataFileChannel;
    private final MappedByteBuffer metaDataBuffer;
    private final ArchiveMetaFileFormatEncoder metaDataWriter;

    private final EpochClock epochClock;
    /**
     * Index is in the range 0:ARCHIVE_FILE_SIZE, except before the first block for this image is received indicated
     * by -1
     */
    private int archivePosition = -1;
    private FileChannel archiveFileChannel;
    private int initialTermId = -1;
    private int initialTermOffset = -1;
    private int lastTermId = -1;
    private int lastTermOffset = -1;

    private boolean closed;
    StreamInstanceArchiveWriter(final File archiveFolder,
                                final EpochClock epochClock,
                                final int streamInstanceId,
                                final int termBufferLength,
                                final StreamInstance streamInstance)
    {
        this.streamInstanceId = streamInstanceId;
        this.archiveFolder = archiveFolder;
        this.termBufferLength = termBufferLength;
        this.epochClock = epochClock;

        this.termsMask = (ArchiveFileUtil.ARCHIVE_FILE_SIZE / termBufferLength) - 1;
        if (((termsMask + 1) & termsMask) != 0)
        {
            throw new IllegalArgumentException(
                "It is assumed the termBufferLength is a power of 2 smaller than 1G and that" +
                "therefore the number of terms in a file is also a power of 2");
        }

        final String archiveMetaFileName = ArchiveFileUtil.archiveMetaFileName(streamInstanceId);
        final File file = new File(archiveFolder, archiveMetaFileName);
        final RandomAccessFile randomAccessFile;
        try
        {
            randomAccessFile = new RandomAccessFile(file, "rw");
            metadataFileChannel = randomAccessFile.getChannel();
            metaDataBuffer = metadataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(metaDataBuffer);
            metaDataWriter = new ArchiveMetaFileFormatEncoder().wrap(unsafeBuffer, 0);

            metaDataWriter.streamInstanceId(streamInstanceId);
            metaDataWriter.startTime(-1);
            metaDataWriter.termBufferLength(termBufferLength);
            metaDataWriter.initialTermId(-1);
            metaDataWriter.initialTermOffset(-1);
            metaDataWriter.lastTermId(-1);
            metaDataWriter.lastTermOffset(-1);
            metaDataWriter.endTime(-1);
            metaDataWriter.sessionId(streamInstance.sessionId());
            metaDataWriter.streamId(streamInstance.streamId());
            metaDataWriter.source(streamInstance.source());
            metaDataWriter.channel(streamInstance.channel());
            metaDataBuffer.force();
        }
        catch (IOException e)
        {
            close();
            LangUtil.rethrowUnchecked(e);
            // the next line is to keep compiler happy with regards to final fields init
            throw new RuntimeException();
        }
    }
    private void newArchiveFile(final int termId)
    {
        final String archiveDataFileName =
            archiveDataFileName(streamInstanceId, initialTermId, termBufferLength, termId);
        final File file = new File(archiveFolder, archiveDataFileName);

        final RandomAccessFile randomAccessFile;
        try
        {
            randomAccessFile = new RandomAccessFile(file, "rwd");
            archiveFileChannel = randomAccessFile.getChannel();
            // presize the file
            archiveFileChannel.position(ArchiveFileUtil.ARCHIVE_FILE_SIZE - 1);
            archiveFileChannel.write(ByteBuffer.wrap(new byte[]{0 }));
            archiveFileChannel.position(0);
        }
        catch (IOException e)
        {
            close();
            LangUtil.rethrowUnchecked(e);
        }
    }


    @Override
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
            prepareWrite(termOffset, termId, archiveOffset);

            fileChannel.transferTo(fileOffset, blockLength, archiveFileChannel);

            writePrologue(termOffset, blockLength, termId, archiveOffset);
        }
        catch (Throwable e)
        {
            close();
            LangUtil.rethrowUnchecked(e);
        }
    }




    @Override
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
            prepareWrite(termOffset, termId, archiveOffset);

            final ByteBuffer src = buffer.byteBuffer().duplicate();
            src.position(termOffset).limit(termOffset + frameLength);
            archiveFileChannel.write(src);

            writePrologue(termOffset, frameLength, termId, archiveOffset);
        }
        catch (Throwable e)
        {
            close();
            LangUtil.rethrowUnchecked(e);
        }
    }

    private void prepareWrite(final int termOffset, final int termId, final int archiveOffset) throws IOException
    {
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
            throw new IllegalArgumentException("It is assumed that archivePosition tracks the calculated " +
                                               "archiveOffset");
        }
        else if (archiveFileChannel.position() != archivePosition)
        {
            throw new IllegalArgumentException("It is assumed that archivePosition tracks the file position");
        }
    }

    private void writePrologue(final int termOffset, final int blockLength, final int termId, final int archiveOffset)
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
            archiveFileChannel.close();
            archivePosition = 0;
            // TODO: allocate ahead files, will also give early indication to low storage
            newArchiveFile(termId + 1);
        }
    }
    public void close()
    {
        if (closed)
        {
            return;
        }
        CloseHelper.quietClose(archiveFileChannel);
        if (metaDataBuffer != null)
        {
            metaDataWriter.endTime(epochClock.time());
            metaDataBuffer.force();
        }
        CloseHelper.quietClose(metadataFileChannel);
        IoUtil.unmap(metaDataBuffer);
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

}

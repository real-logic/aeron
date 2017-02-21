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

import io.aeron.*;
import io.aeron.archiver.messages.*;
import org.agrona.*;
import org.agrona.concurrent.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveFileUtil.archiveDataFileName;

/**
 * Consumes a stream and archives data into files. Each file is 1GB and naming convention is:<br>
 * <i>streamInstaceId.file-index.aaf</i><br>
 * A metadata file:<br>
 * <i>streamInstaceId.meta</i><br>
 * Contains indexing support data and recording info, {@see ArchiveMetaFileFormatDecoder}.
 *
 * For filenames {@see ArchiveFileUtil}.
 *
 * Data in the files is expected to cover from initial positions to last. Each file covers (1GB/term size) terms.
 * To find data by term id and offset you can find the file and position by calculating:<br>
 * <ul>
 * <li> file index    = (term - initial term) / (1GB / term size) </li>
 * <li> file position = offset + term size * [ (term - initial term) % (1GB / term size) ] </li>
 * </ul>
 */
class ImageArchivingSession
{
    enum State
    {
        INIT
        {
            int doWork(final ImageArchivingSession session)
            {
                session.state(ARCHIVING);
                return 1;
            }
        },

        ARCHIVING
        {
            int doWork(final ImageArchivingSession session)
            {
                final int delta = session.image.rawPoll(session::onBlock, ArchiveFileUtil.ARCHIVE_FILE_SIZE);
                if (session.image.isClosed())
                {
                    session.state(CLOSE);
                }
                return delta;
            }
        },

        CLOSE
        {
            int doWork(final ImageArchivingSession session)
            {
                CloseHelper.quietClose(session.archiveFileChannel);
                if (session.metaDataBuffer != null)
                {
                    session.metaDataWriter.endTime(session.epochClock.time());
                    session.metaDataBuffer.force();
                }
                CloseHelper.quietClose(session.metadataFileChannel);
                IoUtil.unmap(session.metaDataBuffer);
                session.state(DONE);
                session.archiverConductor.notifyArchiveStopped(session.streamInstanceId);
                return 1;
            }
        },

        DONE
        {
            int doWork(final ImageArchivingSession session)
            {
                return 0;
            }
        };

        abstract int doWork(ImageArchivingSession session);
    }

    private final ArchiverConductor archiverConductor;
    private final Image image;
    private final int termBufferLength;
    private final EpochClock epochClock;
    private final int termsMask;
    private final int streamInstanceId;

    private final FileChannel metadataFileChannel;
    private final MappedByteBuffer metaDataBuffer;
    private final ArchiveMetaFileFormatDecoder metaDataReader;
    private final ArchiveMetaFileFormatEncoder metaDataWriter;

    private int initialTermId;
    private FileChannel archiveFileChannel;


    private State state = State.INIT;

    /**
     * Index is in the range 0:ARCHIVE_FILE_SIZE, except before the first block for this image is received indicated
     * by -1
     */
    private int archivePosition = -1;

    ImageArchivingSession(final ArchiverConductor archiverConductor, final Image image, final EpochClock epochClock)
    {
        this.archiverConductor = archiverConductor;
        this.image = image;
        this.initialTermId = image.initialTermId();
        this.termBufferLength = image.termBufferLength();
        this.epochClock = epochClock;

        this.termsMask = (ArchiveFileUtil.ARCHIVE_FILE_SIZE / termBufferLength) - 1;
        if (((termsMask + 1) & termsMask) != 0)
        {
            throw new IllegalArgumentException(
                "It is assumed the termBufferLength is a power of 2 smaller than 1G and that" +
                    "therefore the number of terms in a file is also a power of 2");
        }
        final Subscription subscription = image.subscription();
        final int streamId = subscription.streamId();
        final String channel = subscription.channel();
        final int sessionId = image.sessionId();
        final String source = image.sourceIdentity();
        streamInstanceId = archiverConductor.notifyArchiveStarted(source, sessionId, channel, streamId);


        final String archiveMetaFileName = ArchiveFileUtil.archiveMetaFileName(streamInstanceId);
        final File file = new File(archiverConductor.archiveFolder(), archiveMetaFileName);
        final RandomAccessFile randomAccessFile;
        try
        {
            randomAccessFile = new RandomAccessFile(file, "rw");
            metadataFileChannel = randomAccessFile.getChannel();
            metaDataBuffer = metadataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(metaDataBuffer);
            metaDataReader = new ArchiveMetaFileFormatDecoder().wrap(unsafeBuffer, 0, 4096, 0);
            metaDataWriter = new ArchiveMetaFileFormatEncoder().wrap(unsafeBuffer, 0);

            metaDataWriter.streamInstanceId(streamInstanceId);
            metaDataWriter.startTime(epochClock.time());
            metaDataWriter.termBufferLength(termBufferLength);
            metaDataWriter.initialTermId(initialTermId);
            metaDataWriter.initialTermOffset(-1);
            metaDataWriter.lastTermId(initialTermId);
            metaDataWriter.lastTermOffset(-1);
            metaDataWriter.endTime(-1);
            metaDataWriter.sessionId(sessionId);
            metaDataWriter.streamId(streamId);
            metaDataWriter.source(source);
            metaDataWriter.channel(channel);
            metaDataBuffer.force();
        }
        catch (IOException e)
        {
            State.CLOSE.doWork(this);
            LangUtil.rethrowUnchecked(e);
            // the next line is to keep compiler happy with regards to final fields init
            throw new RuntimeException();
        }
    }


    private void newArchiveFile(final int termId)
    {
        final String archiveDataFileName =
            archiveDataFileName(streamInstanceId, initialTermId, termBufferLength, termId);
        final File file = new File(archiverConductor.archiveFolder(), archiveDataFileName);

        final RandomAccessFile randomAccessFile;
        try
        {
            randomAccessFile = new RandomAccessFile(file, "rwd");
            archiveFileChannel = randomAccessFile.getChannel();
            // presize the file
            archiveFileChannel.position(ArchiveFileUtil.ARCHIVE_FILE_SIZE - 1);
            archiveFileChannel.write(ByteBuffer.wrap(new byte[]{ 0 }));
            archiveFileChannel.position(0);
        }
        catch (IOException e)
        {
            close();
            LangUtil.rethrowUnchecked(e);
        }
    }


    private void onBlock(
        final FileChannel fileChannel,
        final long fileOffset,
        final UnsafeBuffer termBuffer,
        final int termOffset,
        final int length,
        final int sessionId,
        final int termId)
    {
        try
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
            final int archiveOffset = ArchiveFileUtil.archiveOffset(
                termOffset, termId, initialTermId, termsMask, termBufferLength);
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
                archiveFileChannel.position(archivePosition);
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

            fileChannel.transferTo(fileOffset, length, archiveFileChannel);
            archivePosition = archiveOffset + length;

            metaDataWriter.lastTermId(termId);
            final int endTermOffset = termOffset + length;
            metaDataWriter.lastTermOffset(endTermOffset);
            metaDataBuffer.force();
            archiverConductor.notifyArchiveProgress(
                streamInstanceId,
                initialTermId,
                metaDataReader.initialTermOffset(),
                termId,
                endTermOffset);
            if (archivePosition == ArchiveFileUtil.ARCHIVE_FILE_SIZE)
            {
                archiveFileChannel.close();
                archivePosition = 0;
                // TODO: allocate ahead files, will also give early indication to low storage
                newArchiveFile(termId + 1);
            }
        }
        catch (Throwable e)
        {
            close();
            LangUtil.rethrowUnchecked(e);
        }
    }

    void close()
    {
        state(State.CLOSE);
    }

    int doWork()
    {
        int workDone = 0;
        State initialState;
        do
        {
            initialState = state();
            workDone += state().doWork(this);
        }
        while (initialState != state());

        return workDone;
    }

    Image image()
    {
        return image;
    }

    State state()
    {
        return state;
    }

    void state(final State state)
    {
        this.state = state;
    }

    int streamInstanceId()
    {
        return streamInstanceId;
    }
}

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

import io.aeron.Image;
import io.aeron.archiver.messages.ArchiveMetaFileFormatDecoder;
import io.aeron.archiver.messages.ArchiveMetaFileFormatEncoder;
import io.aeron.logbuffer.RawBlockHandler;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveFileUtil.archiveDataFileName;

/**
 * Consumes a stream and archives data into files. Each file is 1GB and naming convention is:<br>
 * <i>source.sessionId.channel.streamId.termStart-to-termEnd.aaf</i><br>
 *
 * A metadata file:<br>
 * <i>source.sessionId.channel.streamId.meta</i><br>
 * Contains indexing support data and recording info, {@see ArchiveMetaFileFormatDecoder}.
 *
 * For filenames {@see ArchiveFileUtil}
 *
 * Data in the files is expected to cover from initial positions to last. Each file covers (1GB/term size) terms.
 * To find data by term id and offset you can find the file and position by calculating:<br>
 *  <ul>
 *    <li>file index = (term - initial term)/(1GB/term size)</li>
 *    <li>file position = offset + term size * [ (term - initial term) % (1GB/term size) ] </li>
 *  </ul>
 */
class ImageArchivingSession implements RawBlockHandler
{
    enum State
    {
        INIT
        {
            @Override
            int doWork(ImageArchivingSession session)
            {
                session.state(ARCHIVING);
                return 1;
            }
        },
        ARCHIVING
        {
            @Override
            int doWork(ImageArchivingSession session)
            {
                final int delta = session.image.rawPoll(session, ArchiveFileUtil.ARCHIVE_FILE_SIZE);
                if (session.image.isClosed())
                {
                    session.state(CLOSE);
                }
                return delta;
            }
        },
        CLOSE
        {
            @Override
            int doWork(ImageArchivingSession session)
            {
                CloseHelper.quietClose(session.archiveFileChannel);
                if (session.metaDataBuffer != null)
                {
                    session.metaDataWriter.endTime(System.currentTimeMillis());
                    session.metaDataBuffer.force();
                }
                CloseHelper.quietClose(session.metadataFileChannel);
                session.state(DONE);
                session.archiverConductor.notifyArchiveStopped(session.instanceId);
                return 1;
            }
        },
        DONE
        {
            @Override
            int doWork(ImageArchivingSession session)
            {
                return 0;
            }
        };

        abstract int doWork(ImageArchivingSession session);
    }

    private final ArchiverConductor archiverConductor;
    private final Image image;
    private final String streamInstanceName;
    private final int termBufferLength;
    private final int termsPerFile;
    private final int termsMask;
    private final int instanceId;

    private final FileChannel metadataFileChannel;
    private final MappedByteBuffer metaDataBuffer;
    private final ArchiveMetaFileFormatDecoder metaDataReader;
    private final ArchiveMetaFileFormatEncoder metaDataWriter;

    private int initialTermId;
    private FileChannel archiveFileChannel;


    State state = State.INIT;

    /**
     * Index is in the range 0:ARCHIVE_FILE_SIZE, except before the first block for this image is received indicated
     * by -1
     */
    int index = -1;

    ImageArchivingSession(ArchiverConductor archiverConductor, Image image)
    {
        this.archiverConductor = archiverConductor;
        this.image = image;
        this.streamInstanceName = ArchiveFileUtil.streamInstanceName(image);
        this.initialTermId = image.initialTermId();
        this.termBufferLength = image.termBufferLength();
        this.termsPerFile = ArchiveFileUtil.ARCHIVE_FILE_SIZE / termBufferLength;
        this.termsMask = termsPerFile - 1;
        if ((termsPerFile & termsMask) != 0)
        {
            throw new IllegalArgumentException(
                "It is assumed the termBufferLength is a power of 2 smaller than 1G and that" +
                "therefore the number of terms in a file is also a power of 2");
        }
        instanceId = archiverConductor.notifyArchiveStarted(
            image.sourceIdentity(), image.sessionId(), image.subscription().channel(), image.subscription().streamId());

        final File file = new File(
            archiverConductor.archiveFolder(), ArchiveFileUtil.archiveMetaFileName(streamInstanceName));
        final RandomAccessFile randomAccessFile;
        try
        {
            randomAccessFile = new RandomAccessFile(file, "rw");
            metadataFileChannel = randomAccessFile.getChannel();
            metaDataBuffer = metadataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 64);
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(metaDataBuffer);
            metaDataReader = new ArchiveMetaFileFormatDecoder().wrap(unsafeBuffer, 0, 64, 0);
            metaDataWriter = new ArchiveMetaFileFormatEncoder().wrap(unsafeBuffer, 0);

            metaDataWriter.instanceId(instanceId);
            metaDataWriter.startTime(System.currentTimeMillis());
            metaDataWriter.termBufferLength(termBufferLength);
            metaDataWriter.initialTermId(initialTermId);
            metaDataWriter.initialTermOffset(-1);
            metaDataWriter.lastTermId(initialTermId);
            metaDataWriter.lastTermOffset(-1);
            metaDataWriter.endTime(-1);
            metaDataBuffer.force();
        }
        catch (IOException e)
        {
            close();
            state().doWork(this);
            LangUtil.rethrowUnchecked(e);
            // the next line is to keep compiler happy with regards to final fields init
            throw new RuntimeException();
        }
    }


    private void newArchiveFile(int termId)
    {
        final File file = new File(
            archiverConductor.archiveFolder(), archiveDataFileName(streamInstanceName, termId, termBufferLength));

        final RandomAccessFile randomAccessFile;
        try
        {
            randomAccessFile = new RandomAccessFile(file, "rwd");
            archiveFileChannel = randomAccessFile.getChannel();
            // presize the file
            archiveFileChannel.position(ArchiveFileUtil.ARCHIVE_FILE_SIZE - 1);
            archiveFileChannel.write(ByteBuffer.wrap(new byte[]{0}));
            archiveFileChannel.position(0);
        }
        catch (IOException e)
        {
            close();
            LangUtil.rethrowUnchecked(e);
        }
    }


    public void onBlock(
        FileChannel fileChannel,
        long fileOffset,
        UnsafeBuffer termBuffer,
        int termOffset,
        int length,
        int sessionId,
        int termId)
    {
        try
        {
            if (index == -1 && termId != initialTermId)
            {
                // archiving an ongoing publication
                metaDataWriter.initialTermId(termId);
                initialTermId = termId;
            }
            // TODO: if assumptions below are valid the computation is redundant for all but the first time this
            // TODO: method is called
            final int archiveOffset = ArchiveFileUtil.archiveOffset(
                termOffset, termId, initialTermId, termsMask, termBufferLength);
            if (index == -1)
            {
                newArchiveFile(termId);
                if (archiveFileChannel.position() != 0)
                {
                    throw new IllegalArgumentException(
                        "It is assumed that archiveFileChannel.position() is 0 on first write");
                }

                index = termOffset;
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
                archiveFileChannel.position(index);
            }
            else if (archiveOffset != index)
            {
                throw new IllegalArgumentException("It is assumed that index tracks the calculated archiveOffset");
            }
            else if (archiveFileChannel.position() != index)
            {
                throw new IllegalArgumentException("It is assumed that index tracks the file position");
            }

            fileChannel.transferTo(fileOffset, length, archiveFileChannel);
            index = archiveOffset + length;

            metaDataWriter.lastTermId(termId);
            final int endTermOffset = termOffset + length;
            metaDataWriter.lastTermOffset(endTermOffset);
            archiverConductor.notifyArchiveProgress(
                instanceId, initialTermId, metaDataReader.initialTermOffset(), termId, endTermOffset);
            if (index == ArchiveFileUtil.ARCHIVE_FILE_SIZE)
            {
                archiveFileChannel.close();
                index = 0;
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

    void error()
    {
        //TODO: recovery?
        //TODO: should we close subscription?
        //TODO: notify originator on response channel
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

    void state(State state)
    {
        this.state = state;
    }
}

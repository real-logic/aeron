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
import io.aeron.Publication;
import io.aeron.archiver.messages.ArchiveMetaFileFormatDecoder;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveFileUtil.ARCHIVE_FILE_SIZE;
import static io.aeron.archiver.ArchiveFileUtil.archiveOffset;
import static java.lang.Math.min;

/**
 * replay session should transition from creation, to connected, to replaying/error, to closed/error
 */
class ReplaySession
{
    enum State
    {
        INIT
        {
            int doWork(ReplaySession session)
            {
                // can't do much until plumbing is in place
                if (session.replay.isConnected() && session.control.isConnected())
                {
                    session.state(State.SETUP);
                }
                // failure before SETUP is not reportable...
                else if (session.replay.isClosed() || session.control.isClosed())
                {
                    session.state(CLOSE);
                }

                return 0;
            }
        },
        SETUP
        {
            int doWork(ReplaySession session)
            {
                final Publication control = session.control;
                final String streamInstanceName = session.streamInstance.name();
                final String archiveMetaFileName = ArchiveFileUtil.archiveMetaFileName(streamInstanceName);
                final File archiveMetaFile = new File(session.archiverConductor.archiveFolder(), archiveMetaFileName);
                if (!archiveMetaFile.exists())
                {
                    session.archiverConductor.sendResponse(control, archiveMetaFile.getAbsolutePath() + " not found");
                    session.state(CLOSE);
                    return 1;
                }

                final ArchiveMetaFileFormatDecoder archiveMetaFileFormatDecoder;
                try
                {
                    archiveMetaFileFormatDecoder = ArchiveFileUtil.archiveMetaFileFormatDecoder(archiveMetaFile);
                }
                catch (IOException e)
                {
                    session.archiverConductor.sendResponse(control, archiveMetaFile.getAbsolutePath() + " : failed to map");
                    session.state(CLOSE);
                    LangUtil.rethrowUnchecked(e);
                    return 0;
                }

                final int initialTermId = archiveMetaFileFormatDecoder.initialTermId();
                final int lastTermId = archiveMetaFileFormatDecoder.lastTermId();
                final int termId = session.instanceTerm;
                if ((initialTermId <= lastTermId && (termId < initialTermId || termId > lastTermId)) ||
                    (initialTermId > lastTermId  && (termId > initialTermId || termId < lastTermId)))
                {
                    session.archiverConductor.sendResponse(control, "Requested term (" +
                                                                    termId + ") out of archive range [" +
                                                                    initialTermId + "," + lastTermId + "]");
                    session.state(CLOSE);
                    return 1;
                }
                // TODO: cover termOffset edge cases: range[0, termBufferLength], or [initialOffset, termBufferLength] if first
                //       term or, [0, lastOffset] if lastTerm.

                // TODO: what should we do if the length exceeds range? error or replay what's available?

                // TODO: open ended replay


                final int termBufferLength = archiveMetaFileFormatDecoder.termBufferLength();
                final String archiveDataFileName = ArchiveFileUtil.archiveDataFileName(streamInstanceName, initialTermId,
                                                                                       termBufferLength, termId);
                final File archiveDataFile = new File(session.archiverConductor.archiveFolder(), archiveDataFileName);

                if (!archiveDataFile.exists())
                {
                    session.archiverConductor.sendResponse(control, archiveDataFile.getAbsolutePath() + " not found");
                    session.state(CLOSE);
                    return 1;
                }

                final RandomAccessFile currentDataFile;
                try
                {
                    currentDataFile = new RandomAccessFile(archiveDataFile, "r");
                }
                catch (IOException e)
                {
                    session.archiverConductor.sendResponse(control, archiveDataFile.getAbsolutePath() + " failed to open.");
                    session.state(CLOSE);
                    LangUtil.rethrowUnchecked(e);
                    return 0;
                }
                session.currentDataFile = currentDataFile;
                session.currentDataChannel = currentDataFile.getChannel();

                final int termOffset = session.instanceTermOffset;
                final int archiveOffset = archiveOffset(termOffset, termId, initialTermId, termBufferLength);
                try
                {
                    session.currentDataChannel.position(archiveOffset);
                    session.channelIndex = archiveOffset;
                }
                catch (IOException e)
                {
                    session.archiverConductor.sendResponse(control, "Failed to set position in archive: " + archiveOffset);
                    session.state(CLOSE);
                    LangUtil.rethrowUnchecked(e);
                    return 0;
                }
                // plumbing is secured, we can kick off the replay
                session.archiverConductor.sendResponse(control, null);
                session.state(REPLAY);
                return 1;
            }
        },
        REPLAY
        {
            int doWork(ReplaySession session)
            {
                final long channelIndex = session.channelIndex;
                final long remainingInFile = ARCHIVE_FILE_SIZE - channelIndex;
                final long mtu = session.replay.maxPayloadLength();

                final int claimSize = (int) min(mtu, min(remainingInFile, session.length));

                final FileChannel currentDataChannel = session.currentDataChannel;
                final ArchiverConductor conductor = session.archiverConductor;
                try
                {
                    // TODO: use buffer claim
                    final UnsafeBuffer buffer = session.buffer;

                    final ByteBuffer byteBuffer = buffer.byteBuffer().duplicate();
                    byteBuffer.position(0).limit(claimSize);
                    // Work with buffer directly or wrap with a flyweight
                    final int read = currentDataChannel.read(byteBuffer, channelIndex);

                    if (read != claimSize)
                    {
                        conductor.sendResponse(session.control, "Failed to read " + claimSize + " bytes at position: " +
                                                               channelIndex);
                        session.state(CLOSE);
                        throw new IllegalStateException();
                    }
                    else
                    {
                        while (session.replay.offer(buffer, 0, claimSize) < 0)
                        {
                            //TODO: backoff
                        }
                    }
                    session.channelIndex += claimSize;
                    session.length -= claimSize;
                    if (session.length == 0)
                    {
                        session.state(CLOSE);
                    }
                    else if (session.channelIndex == ARCHIVE_FILE_SIZE)
                    {
                        throw new UnsupportedOperationException("need to implement file rollover");
                    }

                    return claimSize;
                }
                catch (IOException e)
                {
                    conductor.sendResponse(session.control, "Failed to read at position: " + channelIndex);
                    session.state(CLOSE);
                    LangUtil.rethrowUnchecked(e);
                    return 1;
                }
            }
        },
        CLOSE
        {
            int doWork(ReplaySession session)
            {
                session.control.close();
                session.replay.close();
                CloseHelper.quietClose(session.currentDataFile);
                CloseHelper.quietClose(session.currentDataChannel);
                session.state(DONE);
                return 1;
            }
        },
        DONE
        {
            int doWork(ReplaySession session)
            {
                return 0;
            }
        };

        abstract int doWork(ReplaySession session);
    }

    private final StreamInstance streamInstance;

    // replay boundaries
    private final int instanceTerm;
    private final int instanceTermOffset;
    private final long replayLength;

    // 2 way comms setup
    private final Publication control;
    private final Publication replay;
    private final Image image;

    private final ArchiverConductor archiverConductor;

    private final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(1024 * 16,  64));

    private State state = State.INIT;

    private RandomAccessFile currentDataFile;
    private FileChannel currentDataChannel;
    private long channelIndex;
    private long length;

    ReplaySession(StreamInstance streamInstance, int instanceTerm, int instanceTermOffset, long replayLength,
                         Publication replay, Publication control, Image image, ArchiverConductor archiverConductor)
    {
        this.streamInstance = streamInstance;

        this.instanceTerm = instanceTerm;
        this.instanceTermOffset = instanceTermOffset;
        this.replayLength = replayLength;

        this.control = control;
        this.replay = replay;
        this.image = image;
        this.length = replayLength;
        this.archiverConductor = archiverConductor;
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

    private void state(State state)
    {
        this.state = state;
    }

    void close()
    {
        state(State.CLOSE);
    }
}

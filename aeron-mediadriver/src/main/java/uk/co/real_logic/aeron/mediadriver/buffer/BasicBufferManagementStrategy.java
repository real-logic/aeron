/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.mediadriver.buffer;

import uk.co.real_logic.aeron.mediadriver.ChannelMap;
import uk.co.real_logic.aeron.mediadriver.MediaDriver;
import uk.co.real_logic.aeron.mediadriver.UdpDestination;
import uk.co.real_logic.aeron.util.FileMappingConvention;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.collections.TripleLevelMap;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static uk.co.real_logic.aeron.util.FileMappingConvention.termLocation;

/**
 * Basic buffer management where each Term is a file.
 */
public class BasicBufferManagementStrategy implements BufferManagementStrategy
{
    private static final long BUFFER_SIZE = 256 * 1024;

    private final FileChannel templateFile;
    private final File senderDir;
    private final File receiverDir;
    private final ChannelMap<SenderChannelBuffer> srcTermMap;
    private final TripleLevelMap<ByteBuffer> rcvTermMap;
    private final FileMappingConvention fileConvention;

    public BasicBufferManagementStrategy(final String dataDir)
    {
        fileConvention = new FileMappingConvention(dataDir);
        senderDir = fileConvention.senderDir();
        receiverDir = fileConvention.receiverDir();
        srcTermMap = new ChannelMap<>();
        rcvTermMap = new TripleLevelMap<>();
        IoUtil.ensureDirectoryExists(senderDir, "sender");
        IoUtil.ensureDirectoryExists(receiverDir, "receiver");
        templateFile = createTemplateFile(dataDir);
    }

    /**
     * Create a blank, zero'd out file of the correct size.
     * This lets us just use transferTo to initialize the buffers.
     *
     * @param dataDir
     */
    private FileChannel createTemplateFile(final String dataDir)
    {
        final File templateFile = new File(dataDir, "templateFile");
        templateFile.deleteOnExit();
        try
        {
            return IoUtil.createEmptyFile(templateFile, BUFFER_SIZE);
        }
        catch (IOException e)
        {
            throw new IllegalStateException("Cannot create template file", e);
        }
    }

    /**
     * Maps a buffer for a given term, ensuring that a file of the correct size is created.
     */
    public MappedByteBuffer mapTerm(final File rootDir,
                                    final String destination,
                                    final long sessionId,
                                    final long channelId,
                                    final long termId,
                                    final long requiredSize) throws Exception
    {
        final File termIdFile = termLocation(rootDir, sessionId, channelId, termId, true, destination);
        // must be checked at this point, opening a RandomAccessFile will cause this to be true
        final boolean fileExists = termIdFile.exists();
        try (final RandomAccessFile randomAccessFile = new RandomAccessFile(termIdFile, "rw"))
        {
            long size = requiredSize;
            final FileChannel channel = randomAccessFile.getChannel();

            if (fileExists)
            {
                size = randomAccessFile.length();
            }
            else
            {
                long transferred = templateFile.transferTo(0, requiredSize, channel);
                if (transferred != requiredSize)
                {
                    throw new IllegalStateException("Unable to initialize the required size of " + requiredSize);
                }
            }

            return channel.map(READ_WRITE, 0, size);
        }
    }

    @Override
    public ByteBuffer addSenderTerm(final UdpDestination destination,
                                    final long sessionId,
                                    final long channelId,
                                    final long termId) throws Exception
    {
        SenderChannelBuffer channelBuffer = srcTermMap.get(destination, sessionId, channelId);

        if (channelBuffer == null)
        {
            final File file = termLocation(senderDir, sessionId, channelId, termId, true, destination.toString());
            channelBuffer = new SenderChannelBuffer(templateFile, file,
                                                    MediaDriver.COMMAND_BUFFER_SZ + RingBufferDescriptor.TRAILER_SIZE);
            srcTermMap.put(destination, sessionId, channelId, channelBuffer);
        }

        return channelBuffer.newTermBuffer(termId);
    }

    protected interface TermMapper
    {
        MappedByteBuffer mapTerm() throws Exception;
    }

    @Override
    public void removeSenderTerm(final UdpDestination destination,
                                 final long sessionId,
                                 final long channelId,
                                 final long termId)
    {

    }

    @Override
    public void removeSenderChannel(final UdpDestination destination, final long sessionId, final long channelId)
    {
        srcTermMap.remove(destination, sessionId, channelId);
    }

    @Override
    public ByteBuffer addReceiverTerm(final UdpDestination destination,
                                      final long sessionId,
                                      final long channelId,
                                      final long termId) throws Exception
    {
        return null;
    }

    @Override
    public ByteBuffer lookupReceiverTerm(final UdpDestination destination,
                                         final long sessionId,
                                         final long channelId,
                                         final long termId)
    {
        return null;
    }

    // TODO: remove this method after cleaning up onRemoveTerm
    @Override
    public int countChannels(final long sessionId)
    {
        return 1;
    }

    @Override
    public int countSessions(final UdpDestination destination)
    {
        return rcvTermMap.sessionCount();
    }

    @Override
    public int countChannels(final UdpDestination destination, final long sessionId)
    {
        return rcvTermMap.channelCount(sessionId);
    }

    @Override
    public int countTerms(final UdpDestination destination, final long sessionId, final long channelId)
    {
        return rcvTermMap.termCount(sessionId, channelId);
    }
}

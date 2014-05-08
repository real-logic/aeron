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

import uk.co.real_logic.aeron.mediadriver.UdpChannelMap;
import uk.co.real_logic.aeron.mediadriver.UdpDestination;
import uk.co.real_logic.aeron.util.FileMappingConvention;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.collections.ChannelMap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.lang.String.format;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.COMMAND_BUFFER_SZ;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.STATE;
import static uk.co.real_logic.aeron.util.FileMappingConvention.channelLocation;
import static uk.co.real_logic.aeron.util.FileMappingConvention.termLocation;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.BufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.BufferDescriptor.TRAILER_LENGTH;

/**
 * Basic buffer management where each Term is a file.
 */
class MappedBufferManagementStrategy implements BufferManagementStrategy
{
    private static final long LOG_BUFFER_SIZE = COMMAND_BUFFER_SZ + TRAILER_LENGTH;

    private final FileChannel logTemplate;
    private final FileChannel stateTemplate;

    private final File senderDir;
    private final File receiverDir;

    private final UdpChannelMap<MappedBufferRotator> srcTermMap;
    private final UdpChannelMap<MappedBufferRotator> rcvTermMap;
    private final FileMappingConvention fileConvention;

    MappedBufferManagementStrategy(final String dataDir)
    {
        fileConvention = new FileMappingConvention(dataDir);
        senderDir = fileConvention.senderDir();
        receiverDir = fileConvention.receiverDir();
        srcTermMap = new UdpChannelMap<>();
        rcvTermMap = new UdpChannelMap<>();
        IoUtil.ensureDirectoryExists(senderDir, "sender");
        IoUtil.ensureDirectoryExists(receiverDir, "receiver");
        logTemplate = createTemplateFile(dataDir, "logTemplate", LOG_BUFFER_SIZE);
        stateTemplate = createTemplateFile(dataDir, "StateTemplate", STATE_BUFFER_LENGTH);
    }

    public void close()
    {
        try
        {
            logTemplate.close();
            stateTemplate.close();

            srcTermMap.forEach((ChannelMap.ChannelHandler<UdpDestination, MappedBufferRotator>)
                    (final UdpDestination destination,
                     final Long sessionId,
                     final Long channelId,
                     final MappedBufferRotator buffer) -> buffer.close());

            rcvTermMap.forEach((ChannelMap.ChannelHandler<UdpDestination, MappedBufferRotator>)
                    (final UdpDestination destination,
                     final Long sessionId,
                     final Long channelId,
                     final MappedBufferRotator buffer) -> buffer.close());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a blank, zeroed out file of the correct size.
     * This lets us just use transferTo to initialize the buffers.
     *
     * @param dataDir in which template file will be created.
     * @param name
     * @param size
     */
    private FileChannel createTemplateFile(final String dataDir, final String name, final long size)
    {
        final File templateFile = new File(dataDir, name);
        templateFile.deleteOnExit();
        try
        {
            return IoUtil.createEmptyFile(templateFile, size);
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
        final File termIdFile = termLocation(rootDir, sessionId, channelId, termId, true, destination, STATE);
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
                long transferred = logTemplate.transferTo(0, requiredSize, channel);
                if (transferred != requiredSize)
                {
                    throw new IllegalStateException("Unable to initialize the required size of " + requiredSize);
                }
            }

            return channel.map(READ_WRITE, 0, size);
        }
    }

    public BufferRotator addProducerChannel(final UdpDestination destination, final long sessionId, final long channelId) throws Exception
    {
        return addChannel(destination, sessionId, channelId, senderDir, srcTermMap);
    }

    public void removeProducerChannel(final UdpDestination destination, final long sessionId, final long channelId)
            throws IllegalArgumentException
    {
        removeChannel(destination, sessionId, channelId, srcTermMap);
    }

    public void removeConsumerChannel(final UdpDestination destination, final long sessionId, final long channelId)
    {
        removeChannel(destination, sessionId, channelId, rcvTermMap);
    }

    public MappedBufferRotator addConsumerChannel(final UdpDestination destination,
                                                  final long sessionId,
                                                  final long channelId) throws Exception
    {
        return addChannel(destination, sessionId, channelId, receiverDir, rcvTermMap);
    }

    private void removeChannel(final UdpDestination destination,
                               final long sessionId,
                               final long channelId,
                               final UdpChannelMap<MappedBufferRotator> termMap)
    {
        final MappedBufferRotator rotator = termMap.remove(destination, sessionId, channelId);
        if (rotator == null)
        {
            String msg = format("No buffers for %s, session = %d, channel = %d", destination, sessionId, channelId);
            throw new IllegalArgumentException(msg);
        }

        rotator.close();
    }

    private MappedBufferRotator addChannel(final UdpDestination destination,
                                           final long sessionId,
                                           final long channelId,
                                           final File rootDir,
                                           final UdpChannelMap<MappedBufferRotator> termMap)
    {
        MappedBufferRotator channelBuffer = termMap.get(destination, sessionId, channelId);
        if (channelBuffer == null)
        {
            final File dir = channelLocation(rootDir, sessionId, channelId, true, destination.clientAwareUri());
            channelBuffer = new MappedBufferRotator(dir,
                    logTemplate,
                    LOG_BUFFER_SIZE,
                    stateTemplate,
                    STATE_BUFFER_LENGTH);
            termMap.put(destination, sessionId, channelId, channelBuffer);
        }
        return channelBuffer;
    }
}

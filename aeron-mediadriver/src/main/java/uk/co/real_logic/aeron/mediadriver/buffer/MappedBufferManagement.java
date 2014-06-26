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
import java.nio.channels.FileChannel;

import static java.lang.String.format;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.COMMAND_BUFFER_SZ;
import static uk.co.real_logic.aeron.util.FileMappingConvention.channelLocation;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

/**
 * Basic buffer management where each Term is a file.
 */
class MappedBufferManagement implements BufferManagement
{
    public static final long LOG_BUFFER_SIZE = COMMAND_BUFFER_SZ + TRAILER_LENGTH;

    private final FileChannel logTemplate;
    private final FileChannel stateTemplate;

    private final File senderDir;
    private final File receiverDir;

    private final UdpChannelMap<MappedBufferRotator> srcTermMap;
    private final UdpChannelMap<MappedBufferRotator> rcvTermMap;
    private final FileMappingConvention fileConvention;

    MappedBufferManagement(final String dataDir)
    {
        fileConvention = new FileMappingConvention(dataDir);
        senderDir = fileConvention.publicationsDir();
        receiverDir = fileConvention.subscriptionsDir();
        srcTermMap = new UdpChannelMap<>();
        rcvTermMap = new UdpChannelMap<>();
        IoUtil.ensureDirectoryExists(senderDir, "sender");
        IoUtil.ensureDirectoryExists(receiverDir, "receiver");
        logTemplate = createTemplateFile(dataDir, "logTemplate", LOG_BUFFER_SIZE);
        stateTemplate = createTemplateFile(dataDir, "stateTemplate", STATE_BUFFER_LENGTH);
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
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Create a blank, zeroed out file of the correct size.
     * This lets us just use transferTo to initialize the buffers.
     *
     * @param dataDir in which template file will be created.
     * @param name of the file to create.
     * @param size of the file to create.
     */
    private FileChannel createTemplateFile(final String dataDir, final String name, final long size)
    {
        final File templateFile = new File(dataDir, name);
        templateFile.deleteOnExit();
        try
        {
            return IoUtil.createEmptyFile(templateFile, size);
        }
        catch (final IOException ex)
        {
            throw new IllegalStateException("Cannot create template file", ex);
        }
    }

    public BufferRotator addPublication(final UdpDestination destination,
                                        final long sessionId,
                                        final long channelId)
        throws Exception
    {
        return addChannel(destination, sessionId, channelId, senderDir, srcTermMap);
    }

    public void removePublication(final UdpDestination destination, final long sessionId, final long channelId)
        throws IllegalArgumentException
    {
        removeChannel(destination, sessionId, channelId, srcTermMap);
    }

    public void removeSubscriberChannel(final UdpDestination destination, final long sessionId, final long channelId)
    {
        removeChannel(destination, sessionId, channelId, rcvTermMap);
    }

    public BufferRotator addSubscriberChannel(final UdpDestination destination,
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

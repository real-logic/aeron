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

import uk.co.real_logic.aeron.mediadriver.UdpDestination;
import uk.co.real_logic.aeron.util.FileMappingConvention;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.collections.ConnectionMap;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

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

    private final File publicationsDir;
    private final File subscriptionsDir;

    private final ConnectionMap<UdpDestination, MappedBufferRotator> publicationsRotatorMap = new ConnectionMap<>();
    private final ConnectionMap<UdpDestination, MappedBufferRotator> subscriptionsRotatorMap = new ConnectionMap<>();

    MappedBufferManagement(final String dataDir)
    {
        final FileMappingConvention fileConvention = new FileMappingConvention(dataDir);
        publicationsDir = fileConvention.publicationsDir();
        subscriptionsDir = fileConvention.subscriptionsDir();

        IoUtil.ensureDirectoryExists(publicationsDir, FileMappingConvention.PUBLICATIONS);
        IoUtil.ensureDirectoryExists(subscriptionsDir, FileMappingConvention.SUBSCRIPTIONS);

        logTemplate = createTemplateFile(dataDir, "logTemplate", LOG_BUFFER_SIZE);
        stateTemplate = createTemplateFile(dataDir, "stateTemplate", STATE_BUFFER_LENGTH);
    }

    public void close()
    {
        try
        {
            logTemplate.close();
            stateTemplate.close();

            publicationsRotatorMap.forEach(
                (ConnectionMap.ConnectionHandler<UdpDestination, MappedBufferRotator>)
                    (final UdpDestination destination,
                     final Long sessionId,
                     final Long channelId,
                     final MappedBufferRotator bufferRotator) -> bufferRotator.close());

            subscriptionsRotatorMap.forEach(
                (ConnectionMap.ConnectionHandler<UdpDestination, MappedBufferRotator>)
                    (final UdpDestination destination,
                     final Long sessionId,
                     final Long channelId,
                     final MappedBufferRotator bufferRotator) -> bufferRotator.close());
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
     * @param name    of the file to create.
     * @param size    of the file to create.
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

    public BufferRotator addPublication(final UdpDestination destination, final long sessionId, final long channelId)
        throws Exception
    {
        return addPublication(destination, sessionId, channelId, publicationsDir, publicationsRotatorMap);
    }

    public void removePublication(final UdpDestination destination, final long sessionId, final long channelId)
        throws IllegalArgumentException
    {
        removePublication(destination, sessionId, channelId, publicationsRotatorMap);
    }

    public void removeSubscription(final UdpDestination destination, final long sessionId, final long channelId)
    {
        removePublication(destination, sessionId, channelId, subscriptionsRotatorMap);
    }

    public BufferRotator addSubscription(final UdpDestination destination, final long sessionId, final long channelId)
        throws Exception
    {
        return addPublication(destination, sessionId, channelId, subscriptionsDir, subscriptionsRotatorMap);
    }

    private void removePublication(final UdpDestination destination,
                                   final long sessionId,
                                   final long channelId,
                                   final ConnectionMap<UdpDestination, MappedBufferRotator> termMap)
    {
        final MappedBufferRotator bufferRotator = termMap.remove(destination, sessionId, channelId);
        if (bufferRotator == null)
        {
            final String msg = String.format("No buffers for %s, sessionId = %d, channelId = %d",
                                             destination, sessionId, channelId);
            throw new IllegalArgumentException(msg);
        }

        bufferRotator.close();
    }

    private MappedBufferRotator addPublication(final UdpDestination destination,
                                               final long sessionId,
                                               final long channelId,
                                               final File rootDir,
                                               final ConnectionMap<UdpDestination, MappedBufferRotator> rotatorMap)
    {
        MappedBufferRotator bufferRotator = rotatorMap.get(destination, sessionId, channelId);
        if (bufferRotator == null)
        {
            final File dir = channelLocation(rootDir, sessionId, channelId, true, destination.clientAwareUri());
            bufferRotator = new MappedBufferRotator(dir,
                                                    logTemplate,
                                                    LOG_BUFFER_SIZE,
                                                    stateTemplate,
                                                    STATE_BUFFER_LENGTH);
            rotatorMap.put(destination, sessionId, channelId, bufferRotator);
        }

        return bufferRotator;
    }
}

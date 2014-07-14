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
 * Interface for encapsulating the allocation of ByteBuffers for Session, Channel, and Term
 */
public class BufferManagement implements AutoCloseable
{
    public static final int LOG_BUFFER_SIZE = COMMAND_BUFFER_SZ + TRAILER_LENGTH; // TODO: Is this correct?

    private final FileChannel logTemplate;
    private final FileChannel stateTemplate;

    private final File publicationsDir;
    private final File subscriptionsDir;

    private final ConnectionMap<UdpDestination, MappedTermBuffers> publicationTermsMap = new ConnectionMap<>();
    private final ConnectionMap<UdpDestination, MappedTermBuffers> subscriptionTermsMap = new ConnectionMap<>();

    public BufferManagement(final String dataDir)
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

            publicationTermsMap.forEach(
                (ConnectionMap.ConnectionHandler<UdpDestination, MappedTermBuffers>)
                    (final UdpDestination destination,
                     final Long sessionId,
                     final Long channelId,
                     final MappedTermBuffers termBuffers) -> termBuffers.close());

            subscriptionTermsMap.forEach(
                (ConnectionMap.ConnectionHandler<UdpDestination, MappedTermBuffers>)
                    (final UdpDestination destination,
                     final Long sessionId,
                     final Long channelId,
                     final MappedTermBuffers termBuffers) -> termBuffers.close());
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

    public TermBuffers addPublication(final UdpDestination destination, final long sessionId, final long channelId)
        throws Exception
    {
        return add(destination, sessionId, channelId, publicationsDir, publicationTermsMap);
    }

    public void removePublication(final UdpDestination destination, final long sessionId, final long channelId)
        throws IllegalArgumentException
    {
        remove(destination, sessionId, channelId, publicationTermsMap);
    }

    public void removeConnectedSubscription(final UdpDestination destination,
                                            final long sessionId,
                                            final long channelId)
    {
        remove(destination, sessionId, channelId, subscriptionTermsMap);
    }

    public TermBuffers addConnectedSubscription(final UdpDestination destination,
                                                final long sessionId,
                                                final long channelId)
        throws Exception
    {
        return add(destination, sessionId, channelId, subscriptionsDir, subscriptionTermsMap);
    }

    private void remove(final UdpDestination destination,
                        final long sessionId,
                        final long channelId,
                        final ConnectionMap<UdpDestination, MappedTermBuffers> termMap)
    {
        final MappedTermBuffers termBuffers = termMap.remove(destination, sessionId, channelId);
        if (termBuffers == null)
        {
            final String msg = String.format("No buffers for %s, sessionId = %d, channelId = %d",
                                             destination, sessionId, channelId);
            throw new IllegalArgumentException(msg);
        }

        termBuffers.close();
    }

    private MappedTermBuffers add(final UdpDestination destination,
                                  final long sessionId,
                                  final long channelId,
                                  final File rootDir,
                                  final ConnectionMap<UdpDestination, MappedTermBuffers> termsMap)
    {
        MappedTermBuffers termBuffers = termsMap.get(destination, sessionId, channelId);
        if (termBuffers == null)
        {
            final File dir = channelLocation(rootDir, sessionId, channelId, true, destination.clientAwareUri());
            termBuffers = new MappedTermBuffers(dir,
                                                logTemplate,
                                                LOG_BUFFER_SIZE,
                                                stateTemplate,
                                                STATE_BUFFER_LENGTH);
            termsMap.put(destination, sessionId, channelId, termBuffers);
        }

        return termBuffers;
    }

}

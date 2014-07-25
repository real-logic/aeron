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
package uk.co.real_logic.aeron.driver.buffer;

import uk.co.real_logic.aeron.common.IoUtil;
import uk.co.real_logic.aeron.driver.UdpDestination;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.driver.buffer.FileMappingConvention.channelLocation;

/**
 * Factory for creating new {@link TermBuffers} in publications or subscriptions directory as appropriate.
 */
public class TermBuffersFactory implements AutoCloseable
{
    private final FileChannel logTemplate;
    private final FileChannel stateTemplate;

    private final File publicationsDir;
    private final File subscriptionsDir;

    private final int termBufferSize;

    public TermBuffersFactory(final String dataDirectoryName, final int termBufferSize)
    {
        final FileMappingConvention fileConvention = new FileMappingConvention(dataDirectoryName);
        publicationsDir = fileConvention.publicationsDir();
        subscriptionsDir = fileConvention.subscriptionsDir();

        IoUtil.ensureDirectoryExists(publicationsDir, FileMappingConvention.PUBLICATIONS);
        IoUtil.ensureDirectoryExists(subscriptionsDir, FileMappingConvention.SUBSCRIPTIONS);

        this.termBufferSize = termBufferSize;

        logTemplate = createTemplateFile(dataDirectoryName, "logTemplate", termBufferSize);
        stateTemplate = createTemplateFile(dataDirectoryName, "stateTemplate", STATE_BUFFER_LENGTH);
    }

    /**
     * Close the template files.
     */
    public void close()
    {
        try
        {
            logTemplate.close();
            stateTemplate.close();
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Create new {@link TermBuffers} in the publications directory for the supplied triplet.
     *
     * @param udpDestination address on the media to send to.
     * @param sessionId under which transmissions are made.
     * @param channelId within the destination address to separate message flows.
     * @return the newly allocated {@link TermBuffers}
     */
    public TermBuffers newPublication(final UdpDestination udpDestination,
                                      final long sessionId,
                                      final long channelId)
    {
        return newInstance(udpDestination, sessionId, channelId, publicationsDir);
    }

    /**
     * Create new {@link TermBuffers} in the subscriptions directory for the supplied triplet.
     *
     * @param udpDestination address on the media to listened to.
     * @param sessionId under which transmissions are made.
     * @param channelId within the destination address to separate message flows.
     * @return the newly allocated {@link TermBuffers}
     */
    public TermBuffers newConnectedSubscription(final UdpDestination udpDestination,
                                                final long sessionId,
                                                final long channelId)
    {
        return newInstance(udpDestination, sessionId, channelId, subscriptionsDir);
    }

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

    private TermBuffers newInstance(final UdpDestination udpDestination,
                                    final long sessionId,
                                    final long channelId,
                                    final File rootDir)
    {
        final File dir = channelLocation(rootDir, sessionId, channelId, true, udpDestination.canonicalRepresentation());

        return new MappedTermBuffers(dir, logTemplate, termBufferSize, stateTemplate, STATE_BUFFER_LENGTH);
    }
}

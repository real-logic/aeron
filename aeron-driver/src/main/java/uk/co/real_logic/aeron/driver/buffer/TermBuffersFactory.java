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
import uk.co.real_logic.aeron.common.event.EventLogger;

import java.io.File;
import java.nio.channels.FileChannel;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.driver.buffer.FileMappingConvention.streamLocation;

/**
 * Factory for creating new {@link TermBuffers} in publications or subscriptions directory as appropriate.
 */
public class TermBuffersFactory implements AutoCloseable
{
    private final FileChannel logTemplate;
    private final FileChannel stateTemplate;

    private final File publicationsDir;
    private final File subscriptionsDir;

    private final int publicationTermBufferSize;
    private final int maxConnectionTermBufferSize;
    private final EventLogger logger;

    public TermBuffersFactory(
        final String dataDirectoryName,
        final int publicationTermBufferSize,
        final int maxConnectionTermBufferSize,
        final EventLogger logger)
    {
        this.logger = logger;

        final FileMappingConvention fileConvention = new FileMappingConvention(dataDirectoryName);
        publicationsDir = fileConvention.publicationsDir();
        subscriptionsDir = fileConvention.subscriptionsDir();

        IoUtil.ensureDirectoryExists(publicationsDir, FileMappingConvention.PUBLICATIONS);
        IoUtil.ensureDirectoryExists(subscriptionsDir, FileMappingConvention.SUBSCRIPTIONS);

        this.publicationTermBufferSize = publicationTermBufferSize;
        this.maxConnectionTermBufferSize = maxConnectionTermBufferSize;

        final int templateSize = Math.max(publicationTermBufferSize, maxConnectionTermBufferSize);
        logTemplate = createTemplateFile(dataDirectoryName, "logTemplate", templateSize);
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
     * @param channel       address on the media to send to.
     * @param sessionId     under which transmissions are made.
     * @param streamId      within the channel address to separate message flows.
     * @param correlationId to use to distinguish this publication
     * @return the newly allocated {@link TermBuffers}
     */
    public TermBuffers newPublication(
        final String channel,
        final int sessionId,
        final int streamId,
        final long correlationId)
    {
        return newInstance(publicationsDir, channel, sessionId, streamId, correlationId, publicationTermBufferSize);
    }

    /**
     * Create new {@link TermBuffers} in the subscriptions directory for the supplied triplet.
     *
     * @param channel        address on the media to listened to.
     * @param sessionId      under which transmissions are made.
     * @param streamId       within the channel address to separate message flows.
     * @param correlationId  to use to distinguish this connection
     * @param termBufferSize to use for the log buffer
     * @return the newly allocated {@link TermBuffers}
     */
    public TermBuffers newConnection(
        final String channel,
        final int sessionId,
        final int streamId,
        final long correlationId,
        final int termBufferSize)
    {
        if (maxConnectionTermBufferSize < termBufferSize)
        {
            throw new IllegalArgumentException(
                "term buffer size larger than max: " + termBufferSize + " > " + maxConnectionTermBufferSize);
        }

        return newInstance(subscriptionsDir, channel, sessionId, streamId, correlationId, termBufferSize);
    }

    private FileChannel createTemplateFile(final String dataDir, final String name, final long size)
    {
        final File templateFile = new File(dataDir, name);
        templateFile.deleteOnExit();

        return IoUtil.createEmptyFile(templateFile, size);
    }

    private TermBuffers newInstance(
        final File rootDir,
        final String channel,
        final int sessionId,
        final int streamId,
        final long correlationId,
        final int termBufferSize)
    {
        final File dir = streamLocation(rootDir, channel, sessionId, streamId, correlationId, true);

        return new MappedTermBuffers(
            dir, logTemplate, termBufferSize, stateTemplate, STATE_BUFFER_LENGTH, logger);
    }
}

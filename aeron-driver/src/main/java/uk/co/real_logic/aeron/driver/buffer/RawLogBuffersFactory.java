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

import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.aeron.common.event.EventLogger;

import java.io.File;
import java.nio.channels.FileChannel;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.driver.buffer.FileMappingConvention.streamLocation;

/**
 * Factory for creating new {@link RawLogBuffers} in the publications or subscriptions directories as appropriate.
 */
public class RawLogBuffersFactory implements AutoCloseable
{
    private final FileChannel termTemplate;
    private final FileChannel stateTemplate;

    private final File publicationsDir;
    private final File subscriptionsDir;

    private final int publicationTermBufferLength;
    private final int connectionTermBufferMaxLength;
    private final EventLogger logger;

    public RawLogBuffersFactory(
        final String dataDirectoryName,
        final int publicationTermBufferLength,
        final int connectionTermBufferMaxLength,
        final EventLogger logger)
    {
        this.logger = logger;

        final FileMappingConvention fileConvention = new FileMappingConvention(dataDirectoryName);
        publicationsDir = fileConvention.publicationsDir();
        subscriptionsDir = fileConvention.subscriptionsDir();

        IoUtil.ensureDirectoryExists(publicationsDir, FileMappingConvention.PUBLICATIONS);
        IoUtil.ensureDirectoryExists(subscriptionsDir, FileMappingConvention.SUBSCRIPTIONS);

        this.publicationTermBufferLength = publicationTermBufferLength;
        this.connectionTermBufferMaxLength = connectionTermBufferMaxLength;

        final int templateLength = Math.max(publicationTermBufferLength, connectionTermBufferMaxLength);
        termTemplate = createTemplateFile(dataDirectoryName, "termTemplate", templateLength);
        stateTemplate = createTemplateFile(dataDirectoryName, "stateTemplate", STATE_BUFFER_LENGTH);
    }

    /**
     * Close the template files.
     */
    public void close()
    {
        try
        {
            termTemplate.close();
            stateTemplate.close();
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Create new {@link RawLogBuffers} in the publications directory for the supplied triplet.
     *
     * @param channel       address on the media to send to.
     * @param sessionId     under which transmissions are made.
     * @param streamId      within the channel address to separate message flows.
     * @param correlationId to use to distinguish this publication
     * @return the newly allocated {@link RawLogBuffers}
     */
    public RawLogBuffers newPublication(final String channel, final int sessionId, final int streamId, final long correlationId)
    {
        return newInstance(publicationsDir, channel, sessionId, streamId, correlationId, publicationTermBufferLength);
    }

    /**
     * Create new {@link RawLogBuffers} in the subscriptions directory for the supplied triplet.
     *
     * @param channel          address on the media to listened to.
     * @param sessionId        under which transmissions are made.
     * @param streamId         within the channel address to separate message flows.
     * @param correlationId    to use to distinguish this connection
     * @param termBufferLength to use for the log buffer
     * @return the newly allocated {@link RawLogBuffers}
     */
    public RawLogBuffers newConnection(
        final String channel, final int sessionId, final int streamId, final long correlationId, final int termBufferLength)
    {
        if (termBufferLength > connectionTermBufferMaxLength)
        {
            throw new IllegalArgumentException(
                "connection term buffer larger than max length: " + termBufferLength + " > " + connectionTermBufferMaxLength);
        }

        return newInstance(subscriptionsDir, channel, sessionId, streamId, correlationId, termBufferLength);
    }

    private static FileChannel createTemplateFile(final String dataDir, final String name, final long size)
    {
        final File templateFile = new File(dataDir, name);
        templateFile.deleteOnExit();

        return IoUtil.createEmptyFile(templateFile, size);
    }

    private RawLogBuffers newInstance(
        final File rootDir,
        final String channel,
        final int sessionId,
        final int streamId,
        final long correlationId,
        final int termBufferSize)
    {
        final File dir = streamLocation(rootDir, channel, sessionId, streamId, correlationId, true);

        return new MappedRawLogBuffers(dir, termTemplate, termBufferSize, stateTemplate, STATE_BUFFER_LENGTH, logger);
    }
}

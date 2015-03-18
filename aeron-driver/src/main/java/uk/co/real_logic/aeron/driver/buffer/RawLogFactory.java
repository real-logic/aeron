/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
import uk.co.real_logic.agrona.LangUtil;

import java.io.File;
import java.nio.channels.FileChannel;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.driver.buffer.FileMappingConvention.streamLocation;

/**
 * Factory for creating new {@link RawLog} in the publications or subscriptions directories as appropriate.
 */
public class RawLogFactory implements AutoCloseable
{
    private final FileChannel blankTemplate;

    private final File publicationsDir;
    private final File subscriptionsDir;

    private final int publicationTermBufferLength;
    private final int connectionTermBufferMaxLength;
    private final EventLogger logger;

    public RawLogFactory(
        final String dataDirectoryName,
        final int publicationTermBufferLength,
        final int connectionTermBufferMaxLength,
        final EventLogger logger)
    {
        this.logger = logger;

        final FileMappingConvention fileMappingConvention = new FileMappingConvention(dataDirectoryName);
        publicationsDir = fileMappingConvention.publicationsDir();
        subscriptionsDir = fileMappingConvention.subscriptionsDir();

        IoUtil.ensureDirectoryExists(publicationsDir, FileMappingConvention.PUBLICATIONS);
        IoUtil.ensureDirectoryExists(subscriptionsDir, FileMappingConvention.SUBSCRIPTIONS);

        this.publicationTermBufferLength = publicationTermBufferLength;
        this.connectionTermBufferMaxLength = connectionTermBufferMaxLength;

        final int maxTermLength = Math.max(publicationTermBufferLength, connectionTermBufferMaxLength);
        final long blankTemplateLength = computeLogLength(maxTermLength);

        blankTemplate = createTemplateFile(dataDirectoryName, "blankTemplate", blankTemplateLength);
    }

    /**
     * Close the template files.
     */
    public void close()
    {
        try
        {
            blankTemplate.close();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Create new {@link RawLog} in the publications directory for the supplied triplet.
     *
     * @param channel       address on the media to send to.
     * @param sessionId     under which transmissions are made.
     * @param streamId      within the channel address to separate message flows.
     * @param correlationId to use to distinguish this publication
     * @return the newly allocated {@link RawLog}
     */
    public RawLog newPublication(final String channel, final int sessionId, final int streamId, final long correlationId)
    {
        return newInstance(publicationsDir, channel, sessionId, streamId, correlationId, publicationTermBufferLength);
    }

    /**
     * Create new {@link RawLog} in the subscriptions directory for the supplied triplet.
     *
     * @param channel          address on the media to listened to.
     * @param sessionId        under which transmissions are made.
     * @param streamId         within the channel address to separate message flows.
     * @param correlationId    to use to distinguish this connection
     * @param termBufferLength to use for the log buffer
     * @return the newly allocated {@link RawLog}
     */
    public RawLog newConnection(
        final String channel, final int sessionId, final int streamId, final long correlationId, final int termBufferLength)
    {
        if (termBufferLength > connectionTermBufferMaxLength)
        {
            throw new IllegalArgumentException(
                "connection term buffer larger than max length: " + termBufferLength + " > " + connectionTermBufferMaxLength);
        }

        return newInstance(subscriptionsDir, channel, sessionId, streamId, correlationId, termBufferLength);
    }

    private static FileChannel createTemplateFile(final String dataDir, final String name, final long length)
    {
        final File file = new File(dataDir, name);
        file.deleteOnExit();

        return IoUtil.createEmptyFile(file, length);
    }

    private RawLog newInstance(
        final File rootDir,
        final String channel,
        final int sessionId,
        final int streamId,
        final long correlationId,
        final int termBufferLength)
    {
        final File dir = streamLocation(rootDir, channel, sessionId, streamId, correlationId, true);

        return new MappedRawLog(dir, blankTemplate, termBufferLength, logger);
    }
}

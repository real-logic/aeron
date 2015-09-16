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

import uk.co.real_logic.aeron.driver.event.EventLogger;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.LangUtil;

import java.io.File;
import java.nio.channels.FileChannel;

import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.computeLogLength;
import static uk.co.real_logic.aeron.driver.buffer.FileMappingConvention.streamLocation;

/**
 * Factory for creating new {@link RawLog} in the source publications or rebuilt publication images directories as appropriate.
 */
public class RawLogFactory implements AutoCloseable
{
    private final int publicationTermBufferLength;
    private final int imagesTermBufferMaxLength;
    private final int ipcPublicationTermBufferLength;
    private final FileChannel blankTemplate;
    private final File publicationsDir;
    private final File imagesDir;
    private final EventLogger logger;

    public RawLogFactory(
        final String dataDirectoryName,
        final int publicationTermBufferLength,
        final int imagesTermBufferMaxLength,
        final int ipcPublicationTermBufferLength,
        final EventLogger logger)
    {
        this.logger = logger;

        final FileMappingConvention fileMappingConvention = new FileMappingConvention(dataDirectoryName);
        publicationsDir = fileMappingConvention.publicationsDir();
        imagesDir = fileMappingConvention.imagesDir();

        IoUtil.ensureDirectoryExists(publicationsDir, FileMappingConvention.PUBLICATIONS);
        IoUtil.ensureDirectoryExists(imagesDir, FileMappingConvention.IMAGES);

        this.publicationTermBufferLength = publicationTermBufferLength;
        this.imagesTermBufferMaxLength = imagesTermBufferMaxLength;
        this.ipcPublicationTermBufferLength = ipcPublicationTermBufferLength;

        int maxTermLength = Math.max(publicationTermBufferLength, ipcPublicationTermBufferLength);
        maxTermLength = Math.max(maxTermLength, imagesTermBufferMaxLength);
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
    public RawLog newNetworkPublication(final String channel, final int sessionId, final int streamId, final long correlationId)
    {
        return newInstance(publicationsDir, channel, sessionId, streamId, correlationId, publicationTermBufferLength);
    }

    /**
     * Create new {@link RawLog} in the rebuilt publication images directory for the supplied triplet.
     *
     * @param channel          address on the media to listened to.
     * @param sessionId        under which transmissions are made.
     * @param streamId         within the channel address to separate message flows.
     * @param correlationId    to use to distinguish this connection
     * @param termBufferLength to use for the log buffer
     * @return the newly allocated {@link RawLog}
     */
    public RawLog newNetworkedImage(
        final String channel, final int sessionId, final int streamId, final long correlationId, final int termBufferLength)
    {
        if (termBufferLength > imagesTermBufferMaxLength)
        {
            throw new IllegalArgumentException(
                "image term buffer larger than max length: " + termBufferLength + " > " + imagesTermBufferMaxLength);
        }

        return newInstance(imagesDir, channel, sessionId, streamId, correlationId, termBufferLength);
    }

    /**
     * Create a new {@link RawLog} in the publication directory for the supplied parameters.
     *
     * @param sessionId     under which publications are made.
     * @param streamId      within the IPC channel
     * @param correlationId to use to distinguish this shared log
     * @return the newly allocated {@link RawLog}
     */
    public RawLog newDirectPublication(final int sessionId, final int streamId, final long correlationId)
    {
        return newInstance(publicationsDir, "ipc", sessionId, streamId, correlationId, ipcPublicationTermBufferLength);
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
        final File location = streamLocation(rootDir, channel, sessionId, streamId, correlationId);

        return new MappedRawLog(location, blankTemplate, termBufferLength, logger);
    }
}

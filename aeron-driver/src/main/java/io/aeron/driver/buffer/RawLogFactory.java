/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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
package io.aeron.driver.buffer;

import org.agrona.IoUtil;
import org.agrona.concurrent.errors.DistinctErrorLog;

import java.io.File;

import static io.aeron.driver.buffer.FileMappingConvention.streamLocation;

/**
 * Factory for creating new {@link RawLog} in the source publications or rebuilt publication images directories as appropriate.
 */
public class RawLogFactory
{
    private final DistinctErrorLog errorLog;
    private final int maxTermBufferLength;
    private final boolean useSparseFiles;
    private final File publicationsDir;
    private final File imagesDir;

    public RawLogFactory(
        final String dataDirectoryName,
        final int imagesTermBufferMaxLength,
        final boolean useSparseFiles,
        final DistinctErrorLog errorLog)
    {
        this.errorLog = errorLog;
        this.useSparseFiles = useSparseFiles;

        final FileMappingConvention fileMappingConvention = new FileMappingConvention(dataDirectoryName);
        publicationsDir = fileMappingConvention.publicationsDir();
        imagesDir = fileMappingConvention.imagesDir();

        IoUtil.ensureDirectoryExists(publicationsDir, FileMappingConvention.PUBLICATIONS);
        IoUtil.ensureDirectoryExists(imagesDir, FileMappingConvention.IMAGES);

        this.maxTermBufferLength = imagesTermBufferMaxLength;
    }

    /**
     * Create new {@link RawLog} in the publications directory for the supplied triplet.
     *
     * @param channel          address on the media to send to.
     * @param sessionId        under which transmissions are made.
     * @param streamId         within the channel address to separate message flows.
     * @param correlationId    to use to distinguish this publication
     * @param termBufferLength length of each term
     * @return the newly allocated {@link RawLog}
     */
    public RawLog newNetworkPublication(
        final String channel, final int sessionId, final int streamId, final long correlationId, final int termBufferLength)
    {
        return newInstance(publicationsDir, channel, sessionId, streamId, correlationId, termBufferLength);
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
        return newInstance(imagesDir, channel, sessionId, streamId, correlationId, termBufferLength);
    }

    /**
     * Create a new {@link RawLog} in the publication directory for the supplied parameters.
     *
     * @param sessionId        under which publications are made.
     * @param streamId         within the IPC channel
     * @param correlationId    to use to distinguish this shared log
     * @param termBufferLength length of the each term
     * @return the newly allocated {@link RawLog}
     */
    public RawLog newDirectPublication(
        final int sessionId, final int streamId, final long correlationId, final int termBufferLength)
    {
        return newInstance(publicationsDir, "ipc", sessionId, streamId, correlationId, termBufferLength);
    }

    private RawLog newInstance(
        final File rootDir,
        final String channel,
        final int sessionId,
        final int streamId,
        final long correlationId,
        final int termBufferLength)
    {
        validateTermBufferLength(termBufferLength);

        final File location = streamLocation(rootDir, channel, sessionId, streamId, correlationId);

        return new MappedRawLog(location, useSparseFiles, termBufferLength, errorLog);
    }

    private void validateTermBufferLength(int termBufferLength)
    {
        if (termBufferLength < 0 || termBufferLength > maxTermBufferLength)
        {
            throw new IllegalArgumentException(
                "invalid buffer length: " + termBufferLength + " max is " + maxTermBufferLength);
        }
    }
}

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

import java.io.File;

/**
 * Encodes the file mapping convention used by the media driver for log buffer backing files.
 *
 * Root directory is the "aeron.data.dir"
 * Source publications are under "${aeron.data.dir}/publications"
 * Rebuilt publications are under "${aeron.data.dir}/images"
 *
 * Both publications and images share the same structure of "sessionId/streamId/consumptionTermId".
 */
class FileMappingConvention
{
    public static final String PUBLICATIONS = "publications";
    public static final String IMAGES = "images";

    private final File publicationsDir;
    private final File imagesDir;

    FileMappingConvention(final String dataDirName)
    {
        final File dataDir = new File(dataDirName);
        IoUtil.ensureDirectoryExists(dataDir, "data directory");

        publicationsDir = new File(dataDir, PUBLICATIONS);
        imagesDir = new File(dataDir, IMAGES);
    }

    /**
     * Get the directory used for sending files.
     *
     * @return the directory used for files to be sent.
     */
    public File publicationsDir()
    {
        return publicationsDir;
    }

    /**
     * Get the directory used for receiver files. Images of publications that have been rebuilt across the network.
     *
     * @return the directory used for rebuilt files
     */
    public File imagesDir()
    {
        return imagesDir;
    }

    public static File streamLocation(
        final File rootDir,
        final String channel,
        final int sessionId,
        final int streamId,
        final long correlationId)
    {
        final String filename = String.format(
            "%s-%s-%s-%s.logbuffer", channel, toHexString(sessionId), toHexString(streamId), toHexString(correlationId));
        return new File(rootDir, filename);
    }

    private static String toHexString(final int value)
    {
        return String.format("%X", value);
    }

    private static String toHexString(final long value)
    {
        return String.format("%X", value);
    }
}

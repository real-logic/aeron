/*
 * Copyright 2014-2017 Real Logic Ltd.
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

import java.io.File;

/**
 * Encodes the file mapping convention used by the media driver for log buffer backing files.
 * <p>
 * Root directory is the {@link io.aeron.CommonContext#AERON_DIR_PROP_NAME}.
 * Source publications are under "${aeron.dir}/publications"
 * Rebuilt publications are under "${aeron.dir}/images"
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
        final String fileName = channel + '-' +
            Integer.toHexString(sessionId) + '-' +
            Integer.toHexString(streamId) + '-' +
            Long.toHexString(correlationId) + ".logbuffer";

        return new File(rootDir, fileName);
    }
}

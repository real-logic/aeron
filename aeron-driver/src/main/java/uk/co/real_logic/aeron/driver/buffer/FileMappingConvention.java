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
 * Encodes the file mapping convention used by the media driver for exchanging buffer files.
 *
 * Root directory is the "aeron.data.dir"
 * Senders are under "${aeron.data.dir}/publications"
 * Receivers are under "${aeron.data.dir}/subscriptions"
 *
 * Both publications and subscriptions share the same structure of "sessionId/streamId/termId".
 */
class FileMappingConvention
{
    public static final String PUBLICATIONS = "publications";
    public static final String SUBSCRIPTIONS = "subscriptions";

    private final File subscriptionsDir;
    private final File publicationsDir;

    public FileMappingConvention(final String dataDirName)
    {
        final File dataDir = new File(dataDirName);
        IoUtil.ensureDirectoryExists(dataDir, "data directory");

        publicationsDir = new File(dataDir, PUBLICATIONS);
        subscriptionsDir = new File(dataDir, SUBSCRIPTIONS);
    }

    /**
     * Get the directory used for sender files.
     *
     * @return the directory used for sender files
     */
    public File publicationsDir()
    {
        return publicationsDir;
    }

    /**
     * Get the directory used for receiver files.
     *
     * @return the directory used for receiver files
     */
    public File subscriptionsDir()
    {
        return subscriptionsDir;
    }

    public static File streamLocation(
        final File rootDir,
        final String channelDirName,
        final int sessionId,
        final int streamId,
        final long correlationId,
        final boolean createIfMissing)
    {
        final File channelDir = new File(rootDir, channelDirName);
        final File sessionDir = new File(channelDir, toHexString(sessionId));
        final File streamDir = new File(sessionDir, toHexString(streamId));
        final File correlationIdDir = new File(streamDir, toHexString(correlationId));

        if (createIfMissing)
        {
            IoUtil.ensureDirectoryExists(correlationIdDir, "channel");
        }

        return correlationIdDir;
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

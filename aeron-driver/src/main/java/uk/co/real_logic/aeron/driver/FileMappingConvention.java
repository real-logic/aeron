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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.common.IoUtil;

import java.io.File;

/**
 * Encodes the file mapping convention used by the media driver for exchanging buffer files.
 *
 * Root directory is the "aeron.data.dir"
 * Senders are under "${aeron.data.dir}/publications"
 * Receivers are under "${aeron.data.dir}/subscriptions"
 *
 * Both publications and subscriptions share the same structure of "sessionId/channelId/termId".
 */
public class FileMappingConvention
{
    public static final String PUBLICATIONS = "publications";
    public static final String SUBSCRIPTIONS = "subscriptions";

    public enum Type
    {
        LOG,
        STATE
    }

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

    /**
     * Get the file corresponding to a given session/channel/term triple.
     */
    public static File termLocation(final File rootDir,
                                    final long sessionId,
                                    final long channelId,
                                    final long index,
                                    final boolean createIfMissing,
                                    final String destination,
                                    final Type type)
    {
        final File channelDir = channelLocation(rootDir, sessionId, channelId, createIfMissing, destination);
        final String suffix = bufferSuffix(index, type);

        return new File(channelDir, suffix);
    }

    public static String bufferSuffix(final long index, final Type type)
    {
        return Long.toString(index) + "-" + type.name().toLowerCase();
    }

    public static File channelLocation(final File rootDir,
                                       final long sessionId,
                                       final long channelId,
                                       final boolean createIfMissing,
                                       final String destination)
    {
        final File destinationDir = new File(rootDir, destinationToDir(destination));
        final File sessionDir = new File(destinationDir, Long.toString(sessionId));
        final File channelDir = new File(sessionDir, Long.toString(channelId));

        if (createIfMissing)
        {
            IoUtil.ensureDirectoryExists(channelDir, "channel");
        }

        return channelDir;
    }

    public static String destinationToDir(final String destination)
    {
        return destination.replace(':', '_')
                          .replace('/', '_');
    }
}

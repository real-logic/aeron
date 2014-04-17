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
package uk.co.real_logic.aeron.util;

import java.io.File;

/**
 * Encodes the file mapping convention used by both the client and the media driver for exchanging buffer files.
 *
 * Root directory is the "aeron.data.dir"
 * Senders are under "aeron.data.dir/sender"
 * Receivers are under "aeron.data.dir/receiver"
 *
 * Both sources and receivers share the same structure of "sessionId/channelId/termId".
 */
public class FileMappingConvention
{

    public static enum Type { LOG, STATE };

    private final File receiverDir;
    private final File senderDir;
    private final File dataDirFile;

    public FileMappingConvention(final String dataDir)
    {
        dataDirFile = new File(dataDir);
        IoUtil.checkDirectoryExists(dataDirFile, "data directory");
        senderDir = new File(dataDirFile, "sender");
        receiverDir = new File(dataDirFile, "receiver");
    }

    /**
     * Get the directory used for sender files.
     *
     * @return the directory used for sender files
     */
    public File senderDir()
    {
        return senderDir;
    }

    /**
     * Get the directory used for receiver files.
     *
     * @return the directory used for receiver files
     */
    public File receiverDir()
    {
        return receiverDir;
    }

    public File dataDirFile()
    {
        return dataDirFile;
    }

    /**
     * Get the file corresponding to a given session/channel/term triple.
     */
    public static File termLocation(final File rootDir,
                                    final long sessionId,
                                    final long channelId,
                                    final long index,
                                    final boolean createChannelIfMissing,
                                    final String destination,
                                    final Type type)
    {
        final File destinationDir = new File(rootDir, destinationToDir(destination));
        final File sessionDir = new File(destinationDir, Long.toString(sessionId));
        final File channelDir = new File(sessionDir, Long.toString(channelId));
        if (createChannelIfMissing)
        {
            IoUtil.ensureDirectoryExists(channelDir, "channel");
        }
        final String suffix = Long.toString(index) + "-" + type.name().toLowerCase();
        return new File(channelDir, suffix);
    }

    static String destinationToDir(final String destination)
    {
        return destination.replace(':', '_')
                          .replace('/', '_');
    }
}

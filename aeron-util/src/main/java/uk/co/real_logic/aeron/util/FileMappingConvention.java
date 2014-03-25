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

    private final File receiverDir;
    private final File senderDir;

    public FileMappingConvention(final String dataDir)
    {
        final File dataDirFile = new File(dataDir);
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

    /**
     * Get the file corresponding to a given session/channel/term triple.
     */
    public static File termIdFile(final File rootDir,
                                  final long sessionId,
                                  final long channelId,
                                  final long termId,
                                  final boolean createChannelIfMissing)
    {
        final File sessionDir = new File(rootDir, Long.toString(sessionId));
        final File channelDir = new File(sessionDir, Long.toString(channelId));
        if (createChannelIfMissing)
        {
            IoUtil.ensureDirectoryExists(channelDir, "channel");
        }
        return new File(channelDir, Long.toString(termId));
    }

}

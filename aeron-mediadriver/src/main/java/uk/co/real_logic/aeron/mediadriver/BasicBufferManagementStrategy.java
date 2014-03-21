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
package uk.co.real_logic.aeron.mediadriver;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Basic buffer management where each Term is a file
 *
 * Root directory is the "aeron.data.dir"
 * Senders are under "aeron.data.dir/sender"
 * Receivers are under "aeron.data.dir/receiver"
 *
 * Both sources and receivers share the same structure of "sessionId/channelId/termId".
 *
 */
public class BasicBufferManagementStrategy implements BufferManagementStrategy
{
    private static final long BUFFER_SIZE = 256 * 1024;

    private final File senderDir;
    private final File receiverDir;

    public BasicBufferManagementStrategy(final String dataDir)
    {
        final File dataDirFile = new File(dataDir);

        if (!dataDirFile.exists() || !dataDirFile.isDirectory())
        {
            throw new IllegalArgumentException("data directory does not exist or is not a directory: " + dataDir);
        }
        senderDir = new File(dataDirFile, "sender");
        receiverDir = new File(dataDirFile, "receiver");

        if (!senderDir.exists())
        {
            if (!senderDir.mkdir())
            {
                throw new IllegalArgumentException("could not create sender directory: " + dataDir);
            }
        }

        if (!receiverDir.exists())
        {
            if (!receiverDir.mkdir())
            {
                throw new IllegalArgumentException("could not create receiver directory: " + dataDir);
            }
        }
    }

    public static MappedByteBuffer mapTerm(final File rootDir,
                                           final long sessionId,
                                           final long channelId,
                                           final long termId,
                                           final long size) throws Exception
    {
        final File sessionDir = new File(rootDir, Long.toString(sessionId));
        final File channelDir = new File(sessionDir, Long.toString(channelId));
        final File termIdFile = new File(channelDir, Long.toString(termId));

        if (!channelDir.exists())
        {
            channelDir.mkdirs();
        }

        try (final RandomAccessFile randomAccessFile = new RandomAccessFile(termIdFile, "rw"))
        {
            long sz = size;

            if (termIdFile.exists())
            {
                sz = randomAccessFile.length();
            }

            return randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, sz);
        }
    }

    @Override
    public ByteBuffer addSenderChannel(final long sessionId, final long channelId, final long termId) throws Exception
    {
        return mapTerm(senderDir, sessionId, channelId, termId, BUFFER_SIZE);
    }

    @Override
    public ByteBuffer incrementSenderTerm(final long sessionId, final long channelId)
    {
        return null;
    }

    @Override
    public void removeSenderChannel(final long sessionId, final long channelId)
    {

    }

    @Override
    public void removeSenderTerm(final long sessionId, final long channelId, final long termId)
    {

    }

    @Override
    public ByteBuffer addReceiverTerm(final UdpDestination destination, final long sessionId, final long channelId, final long termId)
    {
        return null;
    }

    @Override
    public ByteBuffer incrementReceiverTerm(final UdpDestination destination, final long sessionId, final long channelId)
    {
        return null;
    }
}

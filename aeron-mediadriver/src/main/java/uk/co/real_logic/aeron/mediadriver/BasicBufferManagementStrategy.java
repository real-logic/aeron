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

import uk.co.real_logic.aeron.util.FileMappingConvention;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.collections.TripleLevelMap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Basic buffer management where each Term is a file.
 */
public class BasicBufferManagementStrategy implements BufferManagementStrategy
{
    private static final long BUFFER_SIZE = 256 * 1024;

    private final FileChannel templateFile;
    private final File senderDir;
    private final File receiverDir;
    private final TripleLevelMap<ByteBuffer> srcTermMap;
    private final TripleLevelMap<ByteBuffer> rcvTermMap;
    private final FileMappingConvention fileConvention;

    public BasicBufferManagementStrategy(final String dataDir)
    {
        fileConvention = new FileMappingConvention(dataDir);
        senderDir = fileConvention.senderDir();
        receiverDir = fileConvention.receiverDir();

        IoUtil.ensureDirectoryExists(senderDir, "sender");
        IoUtil.ensureDirectoryExists(receiverDir, "receiver");

        srcTermMap = new TripleLevelMap<>();
        rcvTermMap = new TripleLevelMap<>();
        templateFile = createTemplateFile();
    }

    /**
     * Create a blank, zero'd out file of the correct size.
     * This lets us just use transferTo to initialize the buffers.
     */
    private FileChannel createTemplateFile()
    {
        try
        {
            final File tempFile = File.createTempFile("templateFile", "bin");
            tempFile.deleteOnExit();

            final RandomAccessFile randomAccessFile = new RandomAccessFile(tempFile, "rw");
            final FileChannel templateFile = randomAccessFile.getChannel();
            IoUtil.fill(templateFile, 0, BUFFER_SIZE, (byte) 0);
            return templateFile;
        }
        catch (IOException e)
        {
            throw new IllegalStateException("Unable to create temporary file", e);
        }
    }

    public MappedByteBuffer mapTerm(final File rootDir,
                                    final long sessionId,
                                    final long channelId,
                                    final long termId,
                                    final long requiredSize) throws Exception
    {
        final File termIdFile = FileMappingConvention.termIdFile(rootDir, sessionId, channelId, termId, true);
        // must be checked at this point, opening a RandomAccessFile will cause this to be true
        final boolean fileExists = termIdFile.exists();
        try (final RandomAccessFile randomAccessFile = new RandomAccessFile(termIdFile, "rw"))
        {
            long size = requiredSize;
            final FileChannel channel = randomAccessFile.getChannel();

            if (fileExists)
            {
                size = randomAccessFile.length();
            }
            else
            {
                long transferred = templateFile.transferTo(0, requiredSize, channel);
                if (transferred != requiredSize)
                {
                    throw new IllegalStateException("Unable to initialize the required size of " + requiredSize);
                }
            }

            return channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        }
    }

    @Override
    public void addSenderTerm(final long sessionId, final long channelId, final long termId) throws Exception
    {
        ByteBuffer buffer = srcTermMap.get(sessionId, channelId, termId);

        if (null != buffer)
        {
            throw new IllegalArgumentException(String.format("buffer already exists: %1$s/%2$s/%3$s",
                 sessionId, channelId, termId));
        }

        final MappedByteBuffer term = mapTerm(fileConvention.senderDir(), sessionId, channelId, termId, BUFFER_SIZE);
        srcTermMap.put(sessionId, channelId, termId, term);
    }

    @Override
    public ByteBuffer lookupSenderTerm(final long sessionId, final long channelId, final long termId) throws Exception
    {
        final ByteBuffer buffer = srcTermMap.get(sessionId, channelId, termId);

        if (null == buffer)
        {
            throw new IllegalArgumentException(String.format("buffer does not exist: %1$s/%2$s/%3$s",
                    sessionId, channelId, termId));
        }

        return buffer;
    }

    @Override
    public void removeSenderTerm(final long sessionId, final long channelId, final long termId)
    {
        srcTermMap.remove(sessionId, channelId, termId);
    }

    @Override
    public void removeSenderChannel(final long sessionId, final long channelId)
    {
        srcTermMap.remove(sessionId, channelId);
    }

    @Override
    public void addReceiverTerm(final UdpDestination destination,
                                final long sessionId,
                                final long channelId,
                                final long termId) throws Exception
    {

    }

    @Override
    public ByteBuffer lookupReceiverTerm(final UdpDestination destination,
                                         final long sessionId,
                                         final long channelId,
                                         final long termId)
    {
        return null;
    }

    @Override
    public int countSessions()
    {
        return srcTermMap.sessionCount();
    }

    @Override
    public int countChannels(final long sessionId)
    {
        return srcTermMap.channelCount(sessionId);
    }

    @Override
    public int countTerms(final long sessionId, final long channelId)
    {
        return srcTermMap.termCount(sessionId, channelId);
    }

    @Override
    public int countSessions(final UdpDestination destination)
    {
        return rcvTermMap.sessionCount();
    }

    @Override
    public int countChannels(final UdpDestination destination, final long sessionId)
    {
        return rcvTermMap.channelCount(sessionId);
    }

    @Override
    public int countTerms(final UdpDestination destination, final long sessionId, final long channelId)
    {
        return rcvTermMap.termCount(sessionId, channelId);
    }
}

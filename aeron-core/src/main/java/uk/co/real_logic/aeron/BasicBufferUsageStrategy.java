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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.util.BasicBufferStrategy;
import uk.co.real_logic.aeron.util.FileMappingConvention;
import uk.co.real_logic.aeron.util.collections.TripleLevelMap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * Basic buffer usage where each Term is a file.
 */
public class BasicBufferUsageStrategy extends BasicBufferStrategy implements BufferUsageStrategy
{
    public BasicBufferUsageStrategy(final String dataDir)
    {
        super(dataDir);
    }

    public MappedByteBuffer mapTerm(final File rootDir, final long sessionId, final long channelId, final long termId) throws IOException
    {
        final File termIdFile = FileMappingConvention.termIdFile(rootDir, sessionId, channelId, termId, false);
        if (!termIdFile.exists())
        {
            throw new IllegalStateException("Missing term buffer: " + termIdFile);
        }

        try (final RandomAccessFile randomAccessFile = new RandomAccessFile(termIdFile, "rw"))
        {
            long size = randomAccessFile.length();
            return randomAccessFile.getChannel().map(READ_WRITE, 0, size);
        }
    }

    @Override
    public void onTermAdded(final long sessionId, final long channelId, final long termId, boolean isSender) throws Exception
    {
        TripleLevelMap<ByteBuffer> termMap = isSender ? srcTermMap : rcvTermMap;
        registerTerm(sessionId, channelId, termId, termMap, () ->
        {
            return mapTerm(senderDir, sessionId, channelId, termId);
        });
    }

}

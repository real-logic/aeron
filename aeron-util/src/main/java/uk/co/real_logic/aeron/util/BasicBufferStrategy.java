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

import uk.co.real_logic.aeron.util.collections.TripleLevelMap;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public abstract class BasicBufferStrategy implements BufferStrategy
{

    protected final File senderDir;
    protected final File receiverDir;
    protected final TripleLevelMap<ByteBuffer> srcTermMap;
    protected final TripleLevelMap<ByteBuffer> rcvTermMap;
    protected final FileMappingConvention fileConvention;

    public BasicBufferStrategy(final String dataDir)
    {
        fileConvention = new FileMappingConvention(dataDir);
        senderDir = fileConvention.senderDir();
        receiverDir = fileConvention.receiverDir();
        srcTermMap = new TripleLevelMap<>();
        rcvTermMap = new TripleLevelMap<>();
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

    protected interface TermMapper
    {
        MappedByteBuffer mapTerm() throws Exception;
    }

    protected void registerTerm(final long sessionId,
                                final long channelId,
                                final long termId,
                                final TripleLevelMap<ByteBuffer> termMap,
                                final TermMapper mapper) throws Exception
    {
        ByteBuffer buffer = termMap.get(sessionId, channelId, termId);

        if (null != buffer)
        {
            throw new IllegalArgumentException(String.format("buffer already exists: %1$s/%2$s/%3$s",
                    sessionId, channelId, termId));
        }

        final MappedByteBuffer term = mapper.mapTerm();
        termMap.put(sessionId, channelId, termId, term);
    }

}

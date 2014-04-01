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
package uk.co.real_logic.aeron.mediadriver.buffer;

import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.LongStream;

/**
 * Each sender channel can just rotate 3 memory mapped buffers.
 *
 * This class manages that rotation and the mapping to term ids.
 */
public class SenderChannelBuffer
{

    private static final int RING_SIZE = 3;
    private static final long NO_TERM = -1;

    private final MappedBufferRotator rotator;
    private final Long2ObjectHashMap<MappedByteBuffer> termToBuffer;
    private final long[] termIdQueue;

    private int index;

    public SenderChannelBuffer(final FileChannel templateFile, final File directory, final long bufferSize)
    {
        rotator = new MappedBufferRotator(templateFile, directory, bufferSize);
        termToBuffer = new Long2ObjectHashMap<>();
        termIdQueue = LongStream.range(0, RING_SIZE).map(i -> NO_TERM).toArray();
        index = 0;
    }

    public MappedByteBuffer newTermBuffer(final long termId) throws IOException
    {
        final MappedByteBuffer newBuffer = rotator.rotate();

        index = (index + 1) % RING_SIZE;
        long discardedTermId = termIdQueue[index];
        if (discardedTermId != NO_TERM)
        {
            termToBuffer.remove(discardedTermId);
        }

        termIdQueue[index] = termId;
        termToBuffer.put(termId, newBuffer);
        return newBuffer;
    }

    public MappedByteBuffer get(final long termId)
    {
        return termToBuffer.get(termId);
    }

}

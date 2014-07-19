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
package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.common.IoUtil;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.*;

import static uk.co.real_logic.aeron.common.IoUtil.mapExistingFile;

/**
 * Default mapping buffer lifecycle strategy for the client
 *
 * Note: Not thread-safe - Methods only called from ClientConductor
 */
public class MappedBufferManager implements BufferManager
{
    private final List<LocatedBuffer> buffers = new ArrayList<>();

    public ManagedBuffer newBuffer(final String location, final int offset, final int length)
        throws IOException
    {
        final MappedByteBuffer buffer = mapExistingFile(new File(location), "Term Buffer");
        if (requiresIndirection(buffer, offset, length))
        {
            buffer.position(offset);
            buffer.limit(offset + length);
        }

        buffers.add(new LocatedBuffer(location, buffer));

        return new ManagedBuffer(location, offset, length, new AtomicBuffer(buffer), this);
    }

    private boolean requiresIndirection(final ByteBuffer buffer, final int offset, final int length)
    {
        return offset != 0 || buffer.capacity() != length;
    }

    public int releaseBuffers(final String location, final int offset, final int length)
    {
        final int limit = offset + length;
        int count = 0;
        final Iterator<LocatedBuffer> it = buffers.iterator();

        while (it.hasNext())
        {
            final LocatedBuffer buffer = it.next();
            if (buffer.matches(location, offset, limit))
            {
                buffer.close();
                it.remove();
                count++;
            }
        }

        return count;
    }

    public void close()
    {
        buffers.forEach(LocatedBuffer::close);
    }

    static class LocatedBuffer
    {
        private final String location;
        private final MappedByteBuffer buffer;

        LocatedBuffer(final String location, final MappedByteBuffer buffer)
        {
            this.location = location;
            this.buffer = buffer;
        }

        public boolean matches(final String location, final int offset, final int limit)
        {
            return this.location.equals(location) && buffer.position() == offset && buffer.limit() == limit;
        }

        public void close()
        {
            IoUtil.unmap(buffer);
        }
    }
}

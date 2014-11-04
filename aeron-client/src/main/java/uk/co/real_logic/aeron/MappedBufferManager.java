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

import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static uk.co.real_logic.agrona.IoUtil.mapExistingFile;

/**
 * Default mapping byteBuffer lifecycle strategy for the client
 *
 * Note: Not thread-safe - Methods only called from ClientConductor
 */
class MappedBufferManager implements BufferManager
{
    public ManagedBuffer newBuffer(final String location, final int offset, final int length)
    {
        final MappedByteBuffer buffer = mapExistingFile(new File(location), "Term Buffer");
        if (requiresIndirection(buffer, offset, length))
        {
            buffer.position(offset);
            buffer.limit(offset + length);
        }

        return new MappedManagedBuffer(buffer);
    }

    private boolean requiresIndirection(final ByteBuffer buffer, final int offset, final int length)
    {
        return offset != 0 || buffer.capacity() != length;
    }

    static class MappedManagedBuffer implements ManagedBuffer
    {
        private final MappedByteBuffer byteBuffer;
        private final UnsafeBuffer buffer;

        MappedManagedBuffer(final MappedByteBuffer buffer)
        {
            this.byteBuffer = buffer;
            this.buffer = new UnsafeBuffer(buffer);
        }

        public void close()
        {
            IoUtil.unmap(byteBuffer);
        }

        public UnsafeBuffer buffer()
        {
            return buffer;
        }
    }
}

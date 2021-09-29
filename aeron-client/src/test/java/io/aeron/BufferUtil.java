/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron;

import java.nio.ByteBuffer;

import static org.agrona.UnsafeAccess.UNSAFE;
import static org.junit.jupiter.api.Assertions.*;

final class BufferUtil
{
    private static final long CLEANER_FIELD_OFFSET;
    private static final long THUNK_FIELD_OFFSET;
    private static final long ADDRESS_FIELD_OFFSET;

    static
    {
        try
        {
            final Class<?> bufferClass = Class.forName("java.nio.DirectByteBuffer");
            CLEANER_FIELD_OFFSET = UNSAFE.objectFieldOffset(bufferClass.getDeclaredField("cleaner"));

            Class<?> cleanerClass = null;
            try
            {
                cleanerClass = Class.forName("jdk.internal.ref.Cleaner");
            }
            catch (final ClassNotFoundException ex)
            {
                cleanerClass = Class.forName("sun.misc.Cleaner");
            }

            THUNK_FIELD_OFFSET = UNSAFE.objectFieldOffset(cleanerClass.getDeclaredField("thunk"));

            final Class<?> thunkClass = Class.forName("java.nio.DirectByteBuffer$Deallocator");
            ADDRESS_FIELD_OFFSET = UNSAFE.objectFieldOffset(thunkClass.getDeclaredField("address"));
        }
        catch (final ClassNotFoundException | NoSuchFieldException ex)
        {
            throw new Error("failed to initialize BufferUtil", ex);
        }
    }

    private BufferUtil()
    {
    }

    static void assertDirectByteBufferAllocated(final ByteBuffer buffer)
    {
        assertNotEquals(0, getAddressFromCleaner(buffer));
    }

    static void assertDirectByteBufferFreed(final ByteBuffer buffer)
    {
        assertEquals(0, getAddressFromCleaner(buffer));
    }

    private static long getAddressFromCleaner(final ByteBuffer buffer)
    {
        assertTrue(buffer.isDirect());
        final Class<? extends ByteBuffer> bufferClass = buffer.getClass();
        assertEquals("java.nio.DirectByteBuffer", bufferClass.getName());

        final Object cleaner = UNSAFE.getObject(buffer, CLEANER_FIELD_OFFSET);
        assertNotNull(cleaner);

        final Object thunk = UNSAFE.getObject(cleaner, THUNK_FIELD_OFFSET);
        assertNotNull(thunk);

        return UNSAFE.getLong(thunk, ADDRESS_FIELD_OFFSET);
    }
}

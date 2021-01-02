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
package io.aeron.archive.checksum;

import org.agrona.LangUtil;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.zip.CRC32;

/**
 * Implementation of the {@link Checksum} interface that computes CRC-32 checksum.
 */
final class Crc32 implements Checksum
{
    private static final MethodHandle UPDATE_BYTE_BUFFER;

    static
    {
        Method method = lookupMethod("updateByteBuffer0"); // JDK 9+
        if (null == method)
        {
            method = lookupMethod("updateByteBuffer"); // JDK 8
            if (null == method)
            {
                throw new Error("Failed to find method to compute a checksum from a ByteBuffer on " +
                    CRC32.class.getName() + " class.");
            }
        }

        try
        {
            method.setAccessible(true);
            MethodHandle methodHandle = MethodHandles.lookup().unreflect(method);
            methodHandle = MethodHandles.insertArguments(methodHandle, 0, 0);
            UPDATE_BYTE_BUFFER = methodHandle;
        }
        catch (final Exception ex)
        {
            throw new Error("Failed to acquire method handle for " + method, ex);
        }
    }

    private static Method lookupMethod(final String name)
    {
        try
        {
            return CRC32.class.getDeclaredMethod(name, int.class, long.class, int.class, int.class);
        }
        catch (final NoSuchMethodException ignore)
        {
            return null;
        }
    }

    public static final Crc32 INSTANCE = new Crc32();

    private Crc32()
    {
    }

    public int compute(final long address, final int offset, final int length)
    {
        try
        {
            return (int)UPDATE_BYTE_BUFFER.invokeExact(address, offset, length);
        }
        catch (final Throwable throwable)
        {
            LangUtil.rethrowUnchecked(throwable);
            return -1;
        }
    }
}

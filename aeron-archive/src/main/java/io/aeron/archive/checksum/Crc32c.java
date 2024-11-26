/*
 * Copyright 2014-2024 Real Logic Limited.
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
import java.lang.reflect.Method;
import java.util.zip.CRC32C;

import static java.lang.invoke.MethodHandles.*;
import static java.lang.invoke.MethodType.methodType;

/**
 * Implementation of the {@link Checksum} interface that computes CRC-32C checksum.
 * <p>
 * <em>Note: Available only on JDK 9+.</em>
 * </p>
 */
// TODO: Replace with Agrona classes when released
final class Crc32c implements Checksum
{
    public static final Crc32c INSTANCE = new Crc32c();
    private static final MethodHandle UPDATE_DIRECT_BYTE_BUFFER;

    static
    {
        MethodHandle methodHandle;
        try
        {
            final Method method = CRC32C.class.getDeclaredMethod(
                "updateDirectByteBuffer", int.class, long.class, int.class, int.class);
            method.setAccessible(true);
            final Lookup lookup = lookup();
            methodHandle = lookup.unreflect(method);
            final MethodHandle bitwiseComplement = lookup.findStatic(
                Crc32c.class, "bitwiseComplement", methodType(int.class, int.class));
            // Always invoke with the 0xFFFFFFFF as first argument, i.e. empty CRC value
            methodHandle = insertArguments(methodHandle, 0, 0xFFFFFFFF);
            // Always compute bitwise complement on the result value
            methodHandle = filterReturnValue(methodHandle, bitwiseComplement);
        }
        catch (final Exception ex)
        {
            throw new Error(ex);
        }

        UPDATE_DIRECT_BYTE_BUFFER = methodHandle;
    }

    private static int bitwiseComplement(final int value)
    {
        return ~value;
    }

    private Crc32c()
    {
    }

    public int compute(final long address, final int offset, final int length)
    {
        try
        {
            return (int)UPDATE_DIRECT_BYTE_BUFFER.invokeExact(address, offset, offset + length /* end */);
        }
        catch (final Throwable t)
        {
            LangUtil.rethrowUnchecked(t);
            return -1;
        }
    }
}

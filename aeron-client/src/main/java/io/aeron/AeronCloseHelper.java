/*
 * Copyright 2014-2020 Real Logic Limited.
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

import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.LangUtil;

import java.io.File;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.Collection;

// FIXME: Temporary class until next Agrona release
public final class AeronCloseHelper
{
    private AeronCloseHelper()
    {
    }

    // FIXME: Use Agrona version of this method
    public static void close(final ErrorHandler errorHandler, final AutoCloseable closeable)
    {
        try
        {
            if (null != closeable)
            {
                closeable.close();
            }
        }
        catch (final Throwable ex)
        {
            errorHandler.onError(ex);
        }
    }

    // FIXME: Use Agrona version of this method
    public static void closeAll(final ErrorHandler errorHandler, final Collection<? extends AutoCloseable> closeables)
    {
        if (closeables == null || closeables.isEmpty())
        {
            return;
        }

        for (final AutoCloseable closeable : closeables)
        {
            if (closeable != null)
            {
                try
                {
                    closeable.close();
                }
                catch (final Throwable ex)
                {
                    errorHandler.onError(ex);
                }
            }
        }
    }

    // FIXME: Use Agrona version of this method
    public static void closeAll(final ErrorHandler errorHandler, final AutoCloseable... closeables)
    {
        if (closeables == null || closeables.length == 0)
        {
            return;
        }

        for (final AutoCloseable closeable : closeables)
        {
            if (closeable != null)
            {
                try
                {
                    closeable.close();
                }
                catch (final Throwable ex)
                {
                    errorHandler.onError(ex);
                }
            }
        }
    }

    // FIXME: Use Agrona version of this method
    public static void closeAll(final Collection<? extends AutoCloseable> closeables)
    {
        if (closeables == null || closeables.isEmpty())
        {
            return;
        }

        Throwable error = null;
        for (final AutoCloseable closeable : closeables)
        {
            if (closeable != null)
            {
                try
                {
                    closeable.close();
                }
                catch (final Throwable ex)
                {
                    if (error == null)
                    {
                        error = ex;
                    }
                    else
                    {
                        error.addSuppressed(ex);
                    }
                }
            }
        }

        if (error != null)
        {
            LangUtil.rethrowUnchecked(error);
        }
    }

    // FIXME: Use Agrona version of this method
    public static void closeAll(final AutoCloseable... closeables)
    {
        if (closeables == null || closeables.length == 0)
        {
            return;
        }

        Throwable error = null;
        for (final AutoCloseable closeable : closeables)
        {
            if (closeable != null)
            {
                try
                {
                    closeable.close();
                }
                catch (final Throwable ex)
                {
                    if (error == null)
                    {
                        error = ex;
                    }
                    else
                    {
                        error.addSuppressed(ex);
                    }
                }
            }
        }

        if (error != null)
        {
            LangUtil.rethrowUnchecked(error);
        }
    }

    public static void delete(final File file, final boolean ignoreFailures)
    {
        if (!file.exists())
        {
            return;
        }

        IoUtil.delete(file, ignoreFailures);
    }

    private static final Class<?> DIRECT_BUFFER_INTERFACE;
    private static final MethodHandle DIRECT_BUFFER_CLEANER_ACCESSOR;
    private static final MethodHandle CLEANER_INVOKER;

    static
    {
        try
        {
            DIRECT_BUFFER_INTERFACE = Class.forName("sun.nio.ch.DirectBuffer");
            final MethodHandles.Lookup lookup = MethodHandles.lookup();
            final Class<?> cleanerClass = resolveCleanerClass();
            DIRECT_BUFFER_CLEANER_ACCESSOR = lookup.findVirtual(DIRECT_BUFFER_INTERFACE, "cleaner", MethodType
                .methodType(cleanerClass));
            CLEANER_INVOKER = lookup.findVirtual(cleanerClass, "clean", MethodType.methodType(void.class));
        }
        catch (final ReflectiveOperationException e)
        {
            throw new Error("Failed to init cleaner", e);
        }
    }

    private static Class<?> resolveCleanerClass() throws ClassNotFoundException
    {
        try
        {
            return Class.forName("jdk.internal.ref.Cleaner"); // JDK 9+
        }
        catch (final ClassNotFoundException e)
        {
            return Class.forName("sun.misc.Cleaner");
        }
    }

    public static void free(final DirectBuffer directBuffer)
    {
        if (null == directBuffer)
        {
            return;
        }
        free(directBuffer.byteBuffer());
    }

    public static void free(final ByteBuffer byteBuffer)
    {
        if (null == byteBuffer)
        {
            return;
        }

        if (DIRECT_BUFFER_INTERFACE.isInstance(byteBuffer)) // direct buffer
        {
            try
            {
                final Object cleaner = DIRECT_BUFFER_CLEANER_ACCESSOR.invoke(byteBuffer);
                if (null != cleaner)
                {
                    CLEANER_INVOKER.invoke(cleaner);
                }
            }
            catch (final Throwable throwable)
            {
                LangUtil.rethrowUnchecked(throwable);
            }
        }
    }
}

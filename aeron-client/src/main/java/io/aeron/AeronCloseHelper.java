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

import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.LangUtil;

import java.io.File;
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
}

/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.samples.stress;

import org.agrona.CloseHelper;

import java.util.concurrent.CountDownLatch;

class ShutdownBarrier
{
    private final AutoCloseable closeable;
    private final CountDownLatch startCloseLatch = new CountDownLatch(1);
    private final CountDownLatch completeCloseLatch = new CountDownLatch(1);

    ShutdownBarrier(final AutoCloseable closeable)
    {
        this.closeable = closeable;
        Runtime.getRuntime().addShutdownHook(
            new Thread(this::signal, "shutdown-thread"));
    }

    private void signal()
    {
        startCloseLatch.countDown();
        try
        {
            // A timeout could be specified here to prevent
            // waiting indefinitely during shutdown...
            completeCloseLatch.await();
        }
        catch (final InterruptedException ignore)
        {
        }
    }

    public void run() throws InterruptedException
    {
        try
        {
            startCloseLatch.await();
        }
        finally
        {
            CloseHelper.quietClose(closeable);
            completeCloseLatch.countDown();
        }
    }

    public static void awaitAndCloseOnExit(final AutoCloseable closeable)
        throws InterruptedException
    {
        new ShutdownBarrier(closeable).run();
    }
}
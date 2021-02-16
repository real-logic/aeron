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
package io.aeron.test;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TimeoutTestWatcher implements TestWatcher
{
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
        (runnable) ->
        {
            final Thread t = new Thread(runnable);
            t.setName("Test thread monitor");
            t.setDaemon(true);
            return t;
        });

    private volatile StackTraceElement[] testThreadStackTraceElements = null;

    public void testFailed(final ExtensionContext context, final Throwable cause)
    {
        System.out.printf(
            "Stack Trace from Test thread:%n%s%n",
            Tests.appendStackTrace(new StringBuilder(), testThreadStackTraceElements));
        scheduler.shutdownNow();
    }

    public void testDisabled(final ExtensionContext context, final Optional<String> reason)
    {
        scheduler.shutdownNow();
    }

    public void testSuccessful(final ExtensionContext context)
    {
        scheduler.shutdownNow();
    }

    public void testAborted(final ExtensionContext context, final Throwable cause)
    {
        scheduler.shutdownNow();
    }

    public void monitorTestThread(final Thread testThread)
    {
        scheduler.scheduleAtFixedRate(
            () -> testThreadStackTraceElements = testThread.getStackTrace(), 1, 1, TimeUnit.SECONDS);
    }
}

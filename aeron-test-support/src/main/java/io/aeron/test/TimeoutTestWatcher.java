package io.aeron.test;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TimeoutTestWatcher implements TestWatcher
{
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
        r ->
        {
            final Thread t = new Thread(r);
            t.setName("Test thread monitor");
            t.setDaemon(true);
            return t;
        });

    private volatile StackTraceElement[] testThreadStackTraceElements = null;

    public void testFailed(final ExtensionContext context, final Throwable cause)
    {
        scheduler.shutdownNow();
        System.out.printf(
            "Stack Trace from Test thread:%n%s%n",
            Tests.appendStackTrace(new StringBuilder(), testThreadStackTraceElements));
    }

    public void monitorTestThread(final Thread testThread)
    {
        scheduler.scheduleAtFixedRate(
            () -> testThreadStackTraceElements = testThread.getStackTrace(), 1, 1, TimeUnit.SECONDS);
    }
}

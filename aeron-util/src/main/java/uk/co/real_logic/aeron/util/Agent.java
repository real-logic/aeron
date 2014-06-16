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
package uk.co.real_logic.aeron.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

/**
 * Base agent that is responsible for an ongoing activity which runs in its own thread.
 */
public abstract class Agent implements Runnable, AutoCloseable
{
    private final long sleepPeriodNanos;
    private final CountDownLatch stopLatch;
    private volatile boolean running;

    public Agent(final long sleepPeriodNanos)
    {
        this.sleepPeriodNanos = sleepPeriodNanos;
        this.stopLatch = new CountDownLatch(1);
        this.running = true;
    }

    public void run()
    {
        while (running)
        {
            final boolean hasDoneWork = doWork();

            if (!hasDoneWork)
            {
                LockSupport.parkNanos(sleepPeriodNanos);
            }
        }

        stopLatch.countDown();
    }

    public void close() throws Exception
    {
        running = false;
    }

    /**
     * Stop the running agent. Not waiting for the agent run loop to stop before returning.
     */
    public void stop()
    {
        running = false;
    }

    /**
     * Stop the running agent and wait for run loop to exit or for a timeout to make sure thread has stopped.
     *
     * @param timeout to wait
     * @param timeUnit of timeout
     * @throws TimeoutException if timeout has lapsed
     * @throws InterruptedException if interrupted while waiting
     */
    public void stop(final long timeout, final TimeUnit timeUnit)
        throws TimeoutException, InterruptedException
    {
        stop();
        stopLatch.await(timeout, timeUnit);
    }

    /**
     * An agent should implement this method to do its work.
     *
     * The boolean return value is used for implementing a backoff strategy that can be employed when no work is
     * currently available for the agent to process.
     *
     * @return true if work has been done otherwise false to indicate no work was currently available.
     */
    public abstract boolean doWork();
}

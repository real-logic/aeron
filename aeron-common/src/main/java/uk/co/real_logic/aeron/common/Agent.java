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
package uk.co.real_logic.aeron.common;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Base agent that is responsible for an ongoing activity.
 */
public abstract class Agent implements Runnable, AutoCloseable
{
    private final IdleStrategy idleStrategy;
    private final Consumer<Exception> exceptionHandler;
    private volatile CountDownLatch latch;

    private volatile boolean running;

    /**
     * Create an agent passing in {@link IdleStrategy}
     *
     * @param idleStrategy to use for Agent run loop
     * @param exceptionHandler to be called if an {@link Exception} is encountered
     */
    public Agent(final IdleStrategy idleStrategy, final Consumer<Exception> exceptionHandler)
    {
        this.idleStrategy = idleStrategy;
        this.exceptionHandler = exceptionHandler;
        this.running = true;
    }

    /**
     * Run the Agent logic
     *
     * This method does not return until the run loop is stopped via {@link #close()} or {@link Thread#interrupt()}.
     */
    public void run()
    {
        latch = new CountDownLatch(1);

        while (running)
        {
            try
            {
                final int workCount = doWork();
                idleStrategy.idle(workCount);
            }
            catch (final InterruptedException ignore)
            {
                break;
            }
            catch (final Exception ex)
            {
                exceptionHandler.accept(ex);
            }
        }

        latch.countDown();
    }

    /**
     * Stop the running Agent and cleanup. Not waiting for the agent run loop to stop before returning.
     */
    public final void close()
    {
        running = false;

        if (null != latch)
        {
            while (true)
            {
                try
                {
                    if (latch.await(500, TimeUnit.MILLISECONDS))
                    {
                        break;
                    }
                    System.err.println("timeout await for agent. Retrying...");
                }
                catch (final InterruptedException ignore)
                {
                }

            }
        }

        onClose();
    }

    /**
     * To be overridden by Agents that which to do resource cleanup on close.
     */
    public void onClose()
    {
    }

    /**
     * An agent should implement this method to do its work.
     *
     * The boolean return value is used for implementing a backoff strategy that can be employed when no work is
     * currently available for the agent to process.
     *
     * @return true if work has been done otherwise false to indicate no work was currently available.
     * @throws Exception
     */
    public abstract int doWork() throws Exception;
}

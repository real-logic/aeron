/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import uk.co.real_logic.agrona.concurrent.AtomicCounter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Base agent runner that is responsible for lifecycle of an {@link Agent} and ensuring exceptions are handled.
 */
public class AgentRunner implements Runnable, AutoCloseable
{
    private final IdleStrategy idleStrategy;
    private final Consumer<Throwable> exceptionHandler;
    private final AtomicCounter exceptionCounter;
    private final Agent agent;
    private volatile CountDownLatch latch;
    private volatile Thread thread;

    private volatile boolean running;

    /**
     * Create an agent passing in {@link IdleStrategy}
     *
     * @param idleStrategy     to use for Agent run loop
     * @param exceptionHandler to be called if an {@link Exception} is encountered
     * @param exceptionCounter for reporting how many exceptions have been seen.
     * @param agent            to be run in this thread.
     */
    public AgentRunner(
        final IdleStrategy idleStrategy,
        final Consumer<Throwable> exceptionHandler,
        final AtomicCounter exceptionCounter,
        final Agent agent)
    {
        this.idleStrategy = idleStrategy;
        this.exceptionHandler = exceptionHandler;
        this.exceptionCounter = exceptionCounter;
        this.agent = agent;
    }

    /**
     * The {@link Agent} who's lifecycle is being managed.
     *
     * @return {@link Agent} who's lifecycle is being managed.
     */
    public Agent agent()
    {
        return agent;
    }

    /**
     * Run an {@link Agent}.
     * <p>
     * This method does not return until the run loop is stopped via {@link #close()}.
     */
    public void run()
    {
        running = true;
        latch = new CountDownLatch(1);
        thread = Thread.currentThread();

        final IdleStrategy idleStrategy = this.idleStrategy;
        final Agent agent = this.agent;
        while (running)
        {
            try
            {
                final int workCount = agent.doWork();
                idleStrategy.idle(workCount);
            }
            catch (final InterruptedException ignore)
            {
                Thread.interrupted();
            }
            catch (final Throwable ex)
            {
                if (null != exceptionCounter)
                {
                    exceptionCounter.increment();
                }

                exceptionHandler.accept(ex);
            }
        }

        latch.countDown();
        thread = null;
    }

    /**
     * Stop the running Agent and cleanup. Not waiting for the agent run loop to stop before returning.
     */
    public final void close()
    {
        running = false;
        if (null != thread)
        {
            thread.interrupt();
        }

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
                    Thread.currentThread().interrupt();
                }
            }
        }

        agent.onClose();
    }
}

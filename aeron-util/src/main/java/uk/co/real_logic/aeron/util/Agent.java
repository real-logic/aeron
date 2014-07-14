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

/**
 * Base agent that is responsible for an ongoing activity.
 */
public abstract class Agent implements Runnable, AutoCloseable
{
    private final IdleStrategy idleStrategy;
    private volatile boolean running;

    /**
     * Create an agent passing in {@link IdleStrategy}
     *
     * @param idleStrategy to use for Agent run loop
     */
    public Agent(final IdleStrategy idleStrategy)
    {
        this.idleStrategy = idleStrategy;
        this.running = true;
    }

    /**
     * Run the Agent logic
     *
     * This method does not return until the run loop is stopped via {@link #stop()} or {@link #close()}.
     */
    public void run()
    {
        while (running)
        {
            final int workCount = doWork();

            idleStrategy.idle(workCount);
        }
    }

    /**
     * Stop the running Agent and cleanup. Not waiting for the agent run loop to stop before returning.
     */
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
     * An agent should implement this method to do its work.
     *
     * The boolean return value is used for implementing a backoff strategy that can be employed when no work is
     * currently available for the agent to process.
     *
     * @return true if work has been done otherwise false to indicate no work was currently available.
     */
    public abstract int doWork();
}

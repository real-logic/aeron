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

import java.util.concurrent.locks.LockSupport;

/**
 * Base agent that is responsible for an ongoing activity which runs in its own thread.
 */
public abstract class Agent implements Runnable, AutoCloseable
{
    private final long sleepPeriodNanos;
    private volatile boolean running;

    public Agent(final long sleepPeriodNanos)
    {
        this.sleepPeriodNanos = sleepPeriodNanos;
        running = true;
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
    }

    public void close() throws Exception
    {
        running = false;
    }

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
    public abstract boolean doWork();
}

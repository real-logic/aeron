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
package io.aeron.driver;

import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.status.AtomicCounter;

import java.util.function.Consumer;

import static io.aeron.driver.ThreadingMode.INVOKER;
import static io.aeron.driver.ThreadingMode.SHARED;

abstract class CommandProxy
{
    static final Consumer<Runnable> RUN_TASK = Runnable::run;
    private final ThreadingMode threadingMode;
    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;
    private final AtomicCounter failCount;
    private final boolean notConcurrent;

    CommandProxy(
        final ThreadingMode threadingMode,
        final OneToOneConcurrentArrayQueue<Runnable> commandQueue,
        final AtomicCounter failCount)
    {
        this.threadingMode = threadingMode;
        this.commandQueue = commandQueue;
        this.failCount = failCount;
        notConcurrent = SHARED == threadingMode || INVOKER == threadingMode;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
            "threadingMode=" + threadingMode +
            ", failCount=" + failCount +
            '}';
    }

    final boolean notConcurrent()
    {
        return notConcurrent;
    }

    final ThreadingMode threadingMode()
    {
        return threadingMode;
    }

    final void offer(final Runnable cmd)
    {
        while (!commandQueue.offer(cmd))
        {
            if (!failCount.isClosed())
            {
                failCount.increment();
            }

            Thread.yield();
            if (Thread.currentThread().isInterrupted())
            {
                throw new AgentTerminationException("interrupted");
            }
        }
    }

    final boolean isApplyingBackpressure()
    {
        return commandQueue.remainingCapacity() < 1;
    }
}

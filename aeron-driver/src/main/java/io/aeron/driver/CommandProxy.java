/*
 * Copyright 2014-2024 Real Logic Limited.
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

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.status.AtomicCounter;

import java.util.function.Consumer;

import static io.aeron.driver.ThreadingMode.INVOKER;
import static io.aeron.driver.ThreadingMode.SHARED;

abstract class CommandProxy
{
    private final ThreadingMode threadingMode;
    private final ManyToOneConcurrentLinkedQueue<Runnable> commandQueue;
    private final AtomicCounter failCount;
    private final boolean notConcurrent;

    CommandProxy(
        final ThreadingMode threadingMode,
        final ManyToOneConcurrentLinkedQueue<Runnable> commandQueue,
        final AtomicCounter failCount)
    {
        this.threadingMode = threadingMode;
        this.commandQueue = commandQueue;
        this.failCount = failCount;
        notConcurrent = SHARED == threadingMode || INVOKER == threadingMode;
    }

    /**
     * Is the driver conductor not concurrent with the sender and receiver threads.
     *
     * @return true if the {@link DriverConductor} is on the same thread as the sender and receiver.
     */
    public final boolean notConcurrent()
    {
        return notConcurrent;
    }

    /**
     * Get the threading mode of the driver.
     *
     * @return ThreadingMode of the driver.
     */
    public final ThreadingMode threadingMode()
    {
        return threadingMode;
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

    final void offer(final Runnable cmd)
    {
        if (!commandQueue.offer(cmd))
        {
            // unreachable for ManyToOneConcurrentLinkedQueue
            throw new IllegalStateException(commandQueue.getClass().getSimpleName() + ".offer failed!");
        }
    }

    static int drainQueue(
        final ManyToOneConcurrentLinkedQueue<Runnable> commandQueue,
        final int limit,
        final Consumer<Runnable> consumer)
    {
        int workCount = 0;
        for (int i = 0; i < limit; i++)
        {
            final Runnable command = commandQueue.poll();
            if (null != command)
            {
                consumer.accept(command);
                workCount++;
            }
            else
            {
                break;
            }
        }
        return workCount;
    }
}

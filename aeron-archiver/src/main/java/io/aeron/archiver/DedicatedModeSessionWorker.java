/*
 * Copyright 2014 - 2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.aeron.archiver;

import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.status.AtomicCounter;

class DedicatedModeSessionWorker<T extends Session> extends SessionWorker<T>
{
    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue = new OneToOneConcurrentArrayQueue<>(256);
    private final AtomicCounter errorCounter;
    DedicatedModeSessionWorker(final String roleName, final AtomicCounter errorCounter)
    {
        super(roleName);
        this.errorCounter = errorCounter;
    }

    protected int preWork()
    {
        return commandQueue.drain(Runnable::run);
    }

    protected void addSession(final T session)
    {
        send(() -> super.addSession(session));
    }

    protected void abortSession(final T session)
    {
        send(() -> super.abortSession(session));
    }

    protected void preSessionsClose()
    {
        commandQueue.drain(Runnable::run);
    }

    private void send(final Runnable r)
    {
        while (!commandQueue.offer(r))
        {
            errorCounter.increment();
            Thread.yield();
        }
    }
}

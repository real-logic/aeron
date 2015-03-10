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

package uk.co.real_logic.aeron.common.concurrent;

import sun.misc.Signal;

import java.util.concurrent.locks.LockSupport;

/**
 * One time barrier for blocking a thread until a SIGINT signal is received from the operating system.
 */
public class SigIntBarrier
{
    private final Thread thread;
    private volatile boolean running = true;

    /**
     * Construct and register the barrier ready for use.
     */
    public SigIntBarrier()
    {
        thread = Thread.currentThread();

        Signal.handle(
            new Signal("INT"),
            (signal) ->
            {
                running = false;
                LockSupport.unpark(thread);
            });
    }

    /**
     * Await the reception of the SIGINT signal.
     */
    public void await()
    {
        while (running)
        {
            LockSupport.park();
        }
    }
}

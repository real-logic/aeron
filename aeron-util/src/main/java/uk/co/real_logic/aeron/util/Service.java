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

public abstract class Service implements Runnable, AutoCloseable
{
    private volatile boolean running;

    private final long sleepPeriod;

    public Service(final long sleepPeriod)
    {
        this.sleepPeriod = sleepPeriod;
        running = true;
    }

    public void run()
    {
        while (running)
        {
            process();

            try
            {
                Thread.sleep(sleepPeriod);
            }
            catch (final InterruptedException ex)
            {
                // TODO: logging
                ex.printStackTrace();
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

    public abstract void process();
}

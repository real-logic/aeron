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
package io.aeron.samples.echo;

/**
 * Message to provision a new echo pair.
 */
public class ProvisioningMessage
{
    private final Object mutex = new Object();
    private Object result = null;

    /**
     * Wait for provision message to be processed.
     *
     * @throws InterruptedException if the thread is interrupted while waiting for the response.
     */
    public void await() throws InterruptedException
    {
        synchronized (mutex)
        {
            while (null == result)
            {
                mutex.wait();
            }
        }

        if (result instanceof Exception)
        {
            throw new RuntimeException((Exception)result);
        }
    }

    /**
     * Provide a response for the provisioning request.
     *
     * @param value to be returned to the caller.
     */
    public void complete(final Object value)
    {
        synchronized (mutex)
        {
            result = value;
            mutex.notifyAll();
        }
    }
}

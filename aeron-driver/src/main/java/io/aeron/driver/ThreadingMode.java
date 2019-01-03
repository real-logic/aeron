/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.driver;

/**
 * Threading mode to be employed by the {@link org.agrona.concurrent.Agent}s in the {@link MediaDriver}.
 */
public enum ThreadingMode
{
    /**
     * No threads are started in the {@link MediaDriver}.
     * <p>
     * All 3 {@link org.agrona.concurrent.Agent}s will be composed a {@link org.agrona.concurrent.CompositeAgent} and
     * made runnable via an {@link org.agrona.concurrent.AgentInvoker} in the {@link MediaDriver.Context}.
     */
    INVOKER,

    /**
     * One thread shared by all 3 {@link org.agrona.concurrent.Agent}s.
     */
    SHARED,

    /**
     * One thread shared by both the {@link Sender} and {@link Receiver} agents,
     * plus one for the {@link DriverConductor}.
     */
    SHARED_NETWORK,

    /**
     * 3 Threads, one dedicated to each of the {@link org.agrona.concurrent.Agent}s.
     */
    DEDICATED,
}

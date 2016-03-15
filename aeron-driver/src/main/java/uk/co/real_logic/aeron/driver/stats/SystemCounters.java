/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver.stats;

import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.CountersManager;

import java.util.EnumMap;

/**
 * Aggregate entry point for managing counters of system stats.
 */
public class SystemCounters implements AutoCloseable
{
    private final EnumMap<SystemCounterDescriptor, AtomicCounter> counterByDescriptorMap =
        new EnumMap<>(SystemCounterDescriptor.class);

    /**
     * Construct the counters for this system.
     *
     * @param countersManager which will manage the underlying storage.
     */
    public SystemCounters(final CountersManager countersManager)
    {
        for (final SystemCounterDescriptor descriptor : SystemCounterDescriptor.values())
        {
            counterByDescriptorMap.put(descriptor, descriptor.newCounter(countersManager));
        }
    }

    /**
     * Get the counter for a particular descriptor.
     *
     * @param descriptor by which the counter should be looked up.
     * @return the counter for the given descriptor.
     */
    public AtomicCounter get(final SystemCounterDescriptor descriptor)
    {
        return counterByDescriptorMap.get(descriptor);
    }

    /**
     * Close all the counters.
     */
    public void close()
    {
        counterByDescriptorMap.values().forEach(AtomicCounter::close);
    }
}

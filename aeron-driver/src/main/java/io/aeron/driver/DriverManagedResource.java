/*
 * Copyright 2014-2017 Real Logic Ltd.
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

import org.agrona.ManagedResource;

public interface DriverManagedResource extends ManagedResource
{
    /**
     * Inform resource of timeNs passing and pass it DriverConductor to inform of any state transitions.
     *
     * @param timeNs    now in nanoseconds
     * @param timeMs    now in milliseconds for epoch
     * @param conductor to inform of any state transitions
     */
    void onTimeEvent(long timeNs, long timeMs, DriverConductor conductor);

    /**
     * Has resource reached end of its life and should be reclaimed?
     *
     * @return whether resource has reached end of life or not
     */
    boolean hasReachedEndOfLife();

    /**
     * Increment reference count to this resource.
     *
     * @return new reference count value
     */
    default int incRef()
    {
        return 0;
    }

    /**
     * Decrement reference count to this resource.
     *
     * @return new reference count value
     */
    default int decRef()
    {
        return 0;
    }
}

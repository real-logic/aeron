/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.agrona.ManagedResource;

public interface DriverManagedResource extends ManagedResource
{
    /**
     * Inform resource of time passing and pass it DriverConductor to inform of any state transitions.
     *
     * @param time      now in nanoseconds
     * @param conductor to inform of any state transitions
     */
    void onTimeEvent(final long time, final DriverConductor conductor);

    /**
     * Has resource reached end of its life and should be reclaimed?
     *
     * @return whether resource has reached end of life or not
     */
    boolean hasReachedEndOfLife();
}

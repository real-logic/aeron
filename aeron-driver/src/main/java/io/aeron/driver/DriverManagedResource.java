/*
 * Copyright 2014-2018 Real Logic Ltd.
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

public interface DriverManagedResource
{
    /**
     * Free external resources such as files. if successful then return true.
     *
     * @return true if successful and false if it should be attempted again later.
     */
    default boolean free()
    {
        return true;
    }

    /**
     * Close resources that are not external.
     */
    void close();

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
}

/*
 * Copyright 2014-2022 Real Logic Limited.
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

/**
 * Tracker to handle tracking the duration of a duty cycle.
 */
public class DutyCycleTracker
{
    private long timeOfLastUpdateNs;

    /**
     * Update the last known clock time.
     *
     * @param nowNs to update with.
     */
    public final void update(final long nowNs)
    {
        timeOfLastUpdateNs = nowNs;
    }

    /**
     * Pass measurement to tracker and report updating last known clock time with time.
     *
     * @param nowNs of the measurement.
     */
    public final void measureAndUpdate(final long nowNs)
    {
        final long cycleTimeNs = nowNs - timeOfLastUpdateNs;

        reportMeasurement(cycleTimeNs);
        timeOfLastUpdateNs = nowNs;
    }

    /**
     * Callback called to report duration of cycle.
     *
     * @param durationNs of the duty cycle.
     */
    public void reportMeasurement(final long durationNs)
    {
    }
}

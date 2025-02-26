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
package io.aeron.driver.status;

import io.aeron.driver.DutyCycleTracker;
import org.agrona.concurrent.status.AtomicCounter;

import static java.util.Objects.requireNonNull;

/**
 * Duty cycle tracker that detects when a cycle exceeds a threshold and tracks max cycle time reporting both through
 * counters.
 */
public class DutyCycleStallTracker extends DutyCycleTracker
{
    private final AtomicCounter maxCycleTime;
    private final AtomicCounter cycleTimeThresholdExceededCount;
    private final long cycleTimeThresholdNs;

    /**
     * Create a tracker to track max cycle time and excesses of a threshold.
     *
     * @param maxCycleTime                    counter for tracking.
     * @param cycleTimeThresholdExceededCount counter for tracking.
     * @param cycleTimeThresholdNs            to use for tracking excesses.
     */
    public DutyCycleStallTracker(
        final AtomicCounter maxCycleTime,
        final AtomicCounter cycleTimeThresholdExceededCount,
        final long cycleTimeThresholdNs)
    {
        this.maxCycleTime = requireNonNull(maxCycleTime);
        this.cycleTimeThresholdExceededCount = requireNonNull(cycleTimeThresholdExceededCount);
        this.cycleTimeThresholdNs = cycleTimeThresholdNs;
    }

    /**
     * Get max cycle time counter.
     *
     * @return max cycle time counter.
     */
    public AtomicCounter maxCycleTime()
    {
        return maxCycleTime;
    }

    /**
     * Get threshold exceeded counter.
     *
     * @return threshold exceeded counter.
     */
    public AtomicCounter cycleTimeThresholdExceededCount()
    {
        return cycleTimeThresholdExceededCount;
    }

    /**
     * Get threshold value.
     *
     * @return threshold value.
     */
    public long cycleTimeThresholdNs()
    {
        return cycleTimeThresholdNs;
    }

    /**
     * {@inheritDoc}
     */
    public void reportMeasurement(final long durationNs)
    {
        if (!maxCycleTime.isClosed())
        {
            maxCycleTime.proposeMaxRelease(durationNs);

            if (durationNs > cycleTimeThresholdNs)
            {
                cycleTimeThresholdExceededCount.incrementRelease();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "DutyCycleStallTracker{" +
            "maxCycleTime=" + maxCycleTime +
            ", cycleTimeThresholdExceededCount=" + cycleTimeThresholdExceededCount +
            ", cycleTimeThresholdNs=" + cycleTimeThresholdNs +
            '}';
    }
}

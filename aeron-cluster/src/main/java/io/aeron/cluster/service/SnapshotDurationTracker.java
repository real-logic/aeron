/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.cluster.service;

import org.agrona.concurrent.status.AtomicCounter;


/**
 * Snapshot duration tracker that tracks maximum snapshot duration and also keeps count of how many times a predefined
 * duration threshold is breached.
 */
public class SnapshotDurationTracker
{
    private final AtomicCounter maxSnapshotDuration;
    private final AtomicCounter snapshotDurationThresholdExceededCount;
    private final long durationThresholdNs;
    private long snapshotStartTimeNs = Long.MIN_VALUE;

    /**
     * Create a tracker to track max snapshot duration and breaches of a threshold.
     *
     * @param maxSnapshotDuration                    counter for tracking.
     * @param snapshotDurationThresholdExceededCount counter for tracking.
     * @param durationThresholdNs                    to use for tracking breaches.
     */
    public SnapshotDurationTracker(
        final AtomicCounter maxSnapshotDuration,
        final AtomicCounter snapshotDurationThresholdExceededCount,
        final long durationThresholdNs)
    {
        this.maxSnapshotDuration = maxSnapshotDuration;
        this.snapshotDurationThresholdExceededCount = snapshotDurationThresholdExceededCount;
        this.durationThresholdNs = durationThresholdNs;
    }

    /**
     * Get max snapshot duration counter.
     *
     * @return max snapshot duration counter.
     */
    public AtomicCounter maxSnapshotDuration()
    {
        return maxSnapshotDuration;
    }

    /**
     * Get counter tracking number of times {@link SnapshotDurationTracker#durationThresholdNs} was exceeded.
     *
     * @return duration threshold exceeded counter.
     */
    public AtomicCounter snapshotDurationThresholdExceededCount()
    {
        return snapshotDurationThresholdExceededCount;
    }

    /**
     * Called when snapshotting has started.
     *
     * @param timeNanos snapshot start time in nanoseconds.
     */
    public void onSnapshotBegin(final long timeNanos)
    {
        snapshotStartTimeNs = timeNanos;
    }

    /**
     * Called when snapshot has been taken.
     *
     * @param timeNanos snapshot end time in nanoseconds.
     */
    public void onSnapshotEnd(final long timeNanos)
    {
        if (snapshotStartTimeNs != Long.MIN_VALUE)
        {
            final long snapshotDurationNs = timeNanos - snapshotStartTimeNs;

            if (snapshotDurationNs > durationThresholdNs)
            {
                snapshotDurationThresholdExceededCount.increment();
            }

            maxSnapshotDuration.proposeMax(snapshotDurationNs);
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "SnapshotDurationTracker{" +
            "maxSnapshotDuration=" + maxSnapshotDuration +
            ", snapshotDurationThresholdExceededCount=" + snapshotDurationThresholdExceededCount +
            ", durationThresholdNs=" + durationThresholdNs +
            '}';
    }
}

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
package io.aeron.cluster.service;

import io.aeron.cluster.codecs.ClusterTimeUnit;

import java.util.concurrent.TimeUnit;

/**
 * A clock representing a number of {@link TimeUnit}s since 1 Jan 1970 UTC. Defaults to {@link TimeUnit#MILLISECONDS}.
 * <p>
 * This is the clock used to timestamp sequenced messages for the cluster log and timers. Implementations should
 * be efficient otherwise throughput of the cluster will be impacted due to frequency of use.
 */
@FunctionalInterface
public interface ClusterClock
{
    /**
     * The unit of time returned from the {@link #time()} method.
     *
     * @return the unit of time returned from the {@link #time()} method.
     */
    default TimeUnit timeUnit()
    {
        return TimeUnit.MILLISECONDS;
    }

    /**
     * The count of {@link #timeUnit()}s since 1 Jan 1970 UTC.
     *
     * @return the count of {@link #timeUnit()}s since 1 Jan 1970 UTC.
     */
    long time();

    /**
     * Get the current time in {@link TimeUnit#MILLISECONDS}.
     *
     * @return the current time in {@link TimeUnit#MILLISECONDS}.
     */
    default long timeMillis()
    {
        return timeUnit().toMillis(time());
    }

    /**
     * Get the current time in {@link TimeUnit#MICROSECONDS}.
     *
     * @return the current time in {@link TimeUnit#MICROSECONDS}.
     */
    default long timeMicros()
    {
        return timeUnit().toMicros(time());
    }

    /**
     * Get the current time in {@link TimeUnit#NANOSECONDS}.
     *
     * @return the current time in {@link TimeUnit#NANOSECONDS}.
     */
    default long timeNanos()
    {
        return timeUnit().toNanos(time());
    }

    /**
     * Convert given Cluster time to {@link TimeUnit#NANOSECONDS}.
     *
     * @param time to convert to nanoseconds.
     * @return time in {@link TimeUnit#NANOSECONDS}.
     */
    default long convertToNanos(long time)
    {
        return timeUnit().toNanos(time);
    }

    /**
     * Map {@link TimeUnit} to a corresponding {@link ClusterTimeUnit}.
     *
     * @param timeUnit to map to a corresponding {@link ClusterTimeUnit}.
     * @return a corresponding {@link ClusterTimeUnit}.
     */
    static ClusterTimeUnit map(final TimeUnit timeUnit)
    {
        switch (timeUnit)
        {
            case MILLISECONDS:
                return ClusterTimeUnit.MILLIS;

            case MICROSECONDS:
                return ClusterTimeUnit.MICROS;

            case NANOSECONDS:
                return ClusterTimeUnit.NANOS;

            default:
                throw new IllegalArgumentException("unsupported time unit: " + timeUnit);
        }
    }

    /**
     * Map {@link ClusterTimeUnit} to a corresponding {@link TimeUnit}.
     *
     * @param clusterTimeUnit to map to a corresponding {@link TimeUnit}.
     * @return a corresponding {@link TimeUnit}, if {@link ClusterTimeUnit#NULL_VAL} is passed then defaults
     * to {@link TimeUnit#MILLISECONDS}.
     */
    static TimeUnit map(final ClusterTimeUnit clusterTimeUnit)
    {
        switch (clusterTimeUnit)
        {
            case NULL_VAL:
            case MILLIS:
                return TimeUnit.MILLISECONDS;

            case MICROS:
                return TimeUnit.MICROSECONDS;

            case NANOS:
                return TimeUnit.NANOSECONDS;
        }

        throw new IllegalArgumentException("unsupported time unit: " + clusterTimeUnit);
    }
}

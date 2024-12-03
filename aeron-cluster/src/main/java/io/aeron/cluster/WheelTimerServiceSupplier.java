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
package io.aeron.cluster;

import java.util.concurrent.TimeUnit;

import static org.agrona.BitUtil.findNextPositivePowerOfTwo;

/**
 * Supplies an instance of a {@link WheelTimerService} based on the configuration given to the constructor.
 */
public class WheelTimerServiceSupplier implements TimerServiceSupplier
{
    private final TimeUnit timeUnit;
    private final long startTime;
    private final long tickResolution;
    private final int ticksPerWheel;

    /**
     * Construct the supplier with the necessary parameters to configure the timer wheel.
     *
     * @param timeUnit       for the values used to express the time.  This time unit is used to denote the supplied
     *                       time values.  When the instance is constructed it will use this to convert the supplied
     *                       <code>startTime</code> and <code>tickResolution</code> into the time unit that is being
     *                       used by the cluster clock.
     * @param startTime      for the wheel (in given {@link TimeUnit}).
     * @param tickResolution for the wheel, i.e. how many {@link TimeUnit}s per tick.
     * @param ticksPerWheel  or spokes, for the wheel (must be power of 2).
     */
    public WheelTimerServiceSupplier(
        final TimeUnit timeUnit,
        final long startTime,
        final long tickResolution,
        final int ticksPerWheel)
    {
        this.timeUnit = timeUnit;
        this.startTime = startTime;
        this.tickResolution = tickResolution;
        this.ticksPerWheel = ticksPerWheel;
    }

    /**
     * {@inheritDoc}
     */
    public TimerService newInstance(final TimeUnit clusterTimeUnit, final TimerService.TimerHandler timerHandler)
    {
        final long startTimeInClusterTimeUnits = clusterTimeUnit.convert(startTime, timeUnit);
        final long resolutionInClusterTimeUnits = clusterTimeUnit.convert(tickResolution, timeUnit);

        return new WheelTimerService(
            timerHandler,
            clusterTimeUnit,
            startTimeInClusterTimeUnits,
            findNextPositivePowerOfTwo(resolutionInClusterTimeUnits),
            ticksPerWheel);
    }
}

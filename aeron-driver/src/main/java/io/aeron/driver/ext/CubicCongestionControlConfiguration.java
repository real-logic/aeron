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
package io.aeron.driver.ext;

import java.util.concurrent.TimeUnit;

/**
 * Configuration options to be applied when {@link CubicCongestionControl} is loaded.
 */
public class CubicCongestionControlConfiguration
{
    /**
     * Property name for measuring RTT or using static constant based on initial value.
     *
     * @see CubicCongestionControlConfiguration#INITIAL_RTT_NS_PROP_NAME
     */
    public static final String MEASURE_RTT_PROP_NAME = "aeron.CubicCongestionControl.measureRtt";

    /**
     * Property name for initial RTT measurement in nanoseconds.
     */
    public static final String INITIAL_RTT_NS_PROP_NAME = "aeron.CubicCongestionControl.initialRtt";

    /**
     * Default initial RTT measurement in nanoseconds
     */
    public static final long INITIAL_RTT_NS_DEFAULT = TimeUnit.MICROSECONDS.toNanos(100);

    /**
     * Property name for accounting for TCP behavior in low RTT values after a loss.
     * <p>
     * <b>WARNING:</b> Be aware that throughput utilization becomes important. Turning this on may drastically be off
     * the necessary throughput if utilization is low.
     */
    public static final String TCP_MODE_PROP_NAME = "aeron.CubicCongestionControl.tcpMode";

    public static final boolean MEASURE_RTT = Boolean.getBoolean(MEASURE_RTT_PROP_NAME);
    public static final long INITIAL_RTT_NS = Long.getLong(INITIAL_RTT_NS_PROP_NAME, INITIAL_RTT_NS_DEFAULT);
    public static final boolean TCP_MODE = Boolean.getBoolean(TCP_MODE_PROP_NAME);
}

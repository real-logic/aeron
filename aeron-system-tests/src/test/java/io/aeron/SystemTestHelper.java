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
package io.aeron;

import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;

public class SystemTestHelper
{
    /**
     * Execute a task until a condition is satisfied, or a maximum number of iterations, or a timeout is reached.
     *
     * @param condition         keep executing while true.
     * @param iterationConsumer to be invoked with the iteration count.
     * @param maxIterations     to be executed.
     * @param timeoutNs         to stay within.
     */
    public static void executeUntil(
        final BooleanSupplier condition,
        final IntConsumer iterationConsumer,
        final int maxIterations,
        final long timeoutNs)
    {
        final long startNs = System.nanoTime();
        long nowNs;
        int i = 0;

        do
        {
            iterationConsumer.accept(i);
            nowNs = System.nanoTime();
        }
        while (!condition.getAsBoolean() && ((nowNs - startNs) < timeoutNs) && i++ < maxIterations);
    }

    /**
     * Form URI for spy on channel given.
     *
     * @param channel to spy on
     * @return URI for spy channel.
     */
    public static String spyForChannel(final String channel)
    {
        return CommonContext.SPY_PREFIX + channel;
    }
}

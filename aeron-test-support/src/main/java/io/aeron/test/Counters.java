/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron.test;

import org.agrona.concurrent.status.CountersReader;

import java.util.function.Supplier;

public class Counters
{
    public static void waitForCounterIncrease(final CountersReader reader, final int counterId, final long delta)
    {
        waitForCounterIncrease(reader, counterId, reader.getCounterValue(counterId), delta);
    }

    public static void waitForCounterIncrease(
        final CountersReader reader,
        final int counterId,
        final long initialValue,
        final long delta)
    {
        final long expectedValue = initialValue + delta;
        final Supplier<String> counterMessage = () ->
            "Timed out waiting for counter '" + reader.getCounterLabel(counterId) +
            "' to increase to at least " + expectedValue;

        while (reader.getCounterValue(counterId) < expectedValue)
        {
            Tests.wait(Tests.SLEEP_1_MS, counterMessage);
        }
    }
}

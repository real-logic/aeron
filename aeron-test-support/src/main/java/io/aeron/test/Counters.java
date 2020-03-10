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

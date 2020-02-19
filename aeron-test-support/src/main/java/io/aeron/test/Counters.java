package io.aeron.test;

import org.agrona.concurrent.status.CountersReader;

public class Counters
{
    public static void waitForCounterIncrease(final CountersReader reader, final int counterId, final long delta)
    {
        final long initialValue = reader.getCounterValue(counterId);
        while (reader.getCounterValue(counterId) < initialValue + delta)
        {
            Tests.sleep(1);
        }
    }
}

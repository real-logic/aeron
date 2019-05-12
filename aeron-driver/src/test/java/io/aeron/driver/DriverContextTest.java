package io.aeron.driver;

import org.junit.Test;

public class DriverContextTest
{
    @Test(expected = IllegalStateException.class)
    public void shouldPreventCreatingMultipleDriversWithTheSameContext()
    {
        final MediaDriver.Context ctx = new MediaDriver.Context();

        //noinspection EmptyTryBlock
        try (
            MediaDriver mediaDriver1 = MediaDriver.launchEmbedded(ctx);
            MediaDriver mediaDriver2 = MediaDriver.launchEmbedded(ctx))
        {
        }
    }
}

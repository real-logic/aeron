package io.aeron.driver;

import io.aeron.driver.exceptions.ActiveDriverException;
import io.aeron.exceptions.ConcurrentConcludeException;
import org.junit.Test;

import static org.junit.Assert.fail;

public class DriverContextTest
{
    @Test
    public void shouldPreventCreatingMultipleDriversWithTheSameContext()
    {
        final MediaDriver.Context ctx = new MediaDriver.Context();

        try (
            MediaDriver mediaDriver1 = MediaDriver.launchEmbedded(ctx);
            MediaDriver mediaDriver2 = MediaDriver.launchEmbedded(ctx))
        {
            fail("Exception should be thrown for reuse of the MediaDriver.Context instance");
        }
        catch (final ActiveDriverException | ConcurrentConcludeException e)
        {
            // Expected
        }
    }
}

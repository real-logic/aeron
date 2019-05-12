package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.exceptions.ConcurrentConcludeException;
import org.junit.Test;

public class ClientContextTest
{
    @Test(expected = ConcurrentConcludeException.class)
    public void shouldPreventCreatingMultipleClientsWithTheSameContext()
    {
        try (MediaDriver mediaDriver = MediaDriver.launchEmbedded())
        {
            final Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());

            //noinspection EmptyTryBlock
            try (
                Aeron aeron1 = Aeron.connect(ctx);
                Aeron aeron2 = Aeron.connect(ctx))
            {
            }
        }
    }
}

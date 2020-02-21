package io.aeron.driver;

import io.aeron.driver.MediaDriver.Context;
import org.junit.jupiter.api.Test;

import static io.aeron.driver.Configuration.NAK_MAX_BACKOFF_DEFAULT_NS;
import static io.aeron.driver.Configuration.NAK_MULTICAST_MAX_BACKOFF_PROP_NAME;
import static org.junit.jupiter.api.Assertions.*;

public class MediaDriverContextTest
{
    @Test
    public void nakMulticastMaxBackoffNsDefaultValue()
    {
        final Context context = new Context();
        assertEquals(NAK_MAX_BACKOFF_DEFAULT_NS, context.nakMulticastMaxBackoffNs());
    }

    @Test
    public void nakMulticastMaxBackoffNsValueFromSystemProperty()
    {
        System.setProperty(NAK_MULTICAST_MAX_BACKOFF_PROP_NAME, "333");
        try
        {
            final Context context = new Context();
            assertEquals(333, context.nakMulticastMaxBackoffNs());
        }
        finally
        {
            System.clearProperty(NAK_MULTICAST_MAX_BACKOFF_PROP_NAME);
        }
    }

    @Test
    public void nakMulticastMaxBackoffNsExplicitValue()
    {
        final Context context = new Context();
        context.nakMulticastMaxBackoffNs(Long.MIN_VALUE);
        assertEquals(Long.MIN_VALUE, context.nakMulticastMaxBackoffNs());
    }
}
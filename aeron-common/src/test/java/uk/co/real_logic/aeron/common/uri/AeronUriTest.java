package uk.co.real_logic.aeron.common.uri;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

public class AeronUriTest
{
    @Test
    public void shouldParseSimpleDefaultUri()
    {
        assertParseWithMedia("aeron:udp", "udp");
        assertParseWithMedia("aeron:ipc", "ipc");
        assertParseWithMedia("aeron:", "");
    }

    @Test
    public void shouldRejectUriWithoutAeronPrefix()
    {
        assertInvalid(":udp");
        assertInvalid("aeron");
        assertInvalid("aron:");
        assertInvalid("eeron:");
    }

    private void assertParseWithMedia(String uriStr, String media)
    {
        final AeronUri uri = AeronUri.parse(uriStr);
        assertThat(uri.getScheme(), is("aeron"));
        assertThat(uri.getMedia(), is(media));
    }

    private static void assertInvalid(String string)
    {
        try
        {
            AeronUri.parse(string);
            fail(IllegalArgumentException.class.getName() + " not thrown");
        }
        catch (final IllegalArgumentException e)
        {
            // No-op
        }
    }
}

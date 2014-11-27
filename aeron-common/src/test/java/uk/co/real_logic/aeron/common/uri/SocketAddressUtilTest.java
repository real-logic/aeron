package uk.co.real_logic.aeron.common.uri;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.junit.Test;

public class SocketAddressUtilTest
{
    @Test
    public void shouldParseIpV4AddressAndPort() throws Exception
    {
        assertCorrectParse("192.168.1.20", 55);
    }

    @Test
    public void shouldParseHostAddressAndPort() throws Exception
    {
        assertCorrectParse("localhost", 55);
        assertCorrectParse("example.com", 55);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectOnInvalidPort() throws Exception
    {
        SocketAddressUtil.parse("192.168.1.20:aa");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectOnMissingPort() throws Exception
    {
        SocketAddressUtil.parse("192.168.1.20");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectOnEmptyPort() throws Exception
    {
        SocketAddressUtil.parse("192.168.1.20:");
    }

    private void assertCorrectParse(final String host, final int port) throws UnknownHostException
    {
        final InetSocketAddress address = SocketAddressUtil.parse(host + ":" + port);
        assertThat(address.getAddress(), is(InetAddress.getByName(host)));
        assertThat(address.getPort(), is(port));
    }

}

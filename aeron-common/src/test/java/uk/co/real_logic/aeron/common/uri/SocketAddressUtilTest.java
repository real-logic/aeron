/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.common.uri;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import java.net.Inet6Address;
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

    @Test
    public void shouldParseIpV6() throws Exception
    {
        assertCorrectParseIpV6("::1", 54321);
    }

    @Test
    public void shouldParseWithScope() throws Exception
    {
        InetSocketAddress address = SocketAddressUtil.parse("[::1%12~_.-34]:1234");
        assertThat(address.getAddress(), instanceOf(Inet6Address.class));
    }

    private void assertCorrectParse(final String host, final int port) throws UnknownHostException
    {
        final InetSocketAddress address = SocketAddressUtil.parse(host + ":" + port);
        assertThat(address.getAddress(), is(InetAddress.getByName(host)));
        assertThat(address.getPort(), is(port));
    }

    private void assertCorrectParseIpV6(final String host, final int port) throws UnknownHostException
    {
        final InetSocketAddress address = SocketAddressUtil.parse("[" + host + "]:" + port);
        assertThat(address.getAddress(), is(InetAddress.getByName(host)));
        assertThat(address.getPort(), is(port));
    }
}

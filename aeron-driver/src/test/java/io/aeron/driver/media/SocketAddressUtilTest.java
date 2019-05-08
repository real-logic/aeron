/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.driver.media;

import org.junit.Test;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

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
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectOnInvalidPort()
    {
        SocketAddressUtil.parse("192.168.1.20:aa");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectOnInvalidPort2()
    {
        SocketAddressUtil.parse("192.168.1.20::123");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectOnMissingPort()
    {
        SocketAddressUtil.parse("192.168.1.20");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectOnEmptyPort()
    {
        SocketAddressUtil.parse("192.168.1.20:");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectOnEmptyIpV6Port()
    {
        SocketAddressUtil.parse("[::1]:");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectOnInvalidIpV6()
    {
        SocketAddressUtil.parse("[FG07::789:1:0:0:3]:111");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectOnInvalidIpV6Scope()
    {
        SocketAddressUtil.parse("[FC07::789:1:0:0:3%^]:111");
    }

    @Test
    public void shouldParseIpV6() throws Exception
    {
        assertCorrectParseIpV6("::1", 54321);
        assertCorrectParseIpV6("FC07::789:1:0:0:3", 54321);
        assertCorrectParseIpV6("fc07::789:1:0:0:3", 54321);
    }

    @Test
    public void shouldParseWithScope()
    {
        final InetSocketAddress address = SocketAddressUtil.parse("[::1%12~_.-34]:1234");
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

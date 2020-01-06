/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver.media;

import org.junit.jupiter.api.Test;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SocketAddressParserTest
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

    @Test
    public void shouldRejectOnInvalidPort()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse("192.168.1.20:aa"));
    }

    @Test
    public void shouldRejectOnInvalidPort2()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse("192.168.1.20::123"));
    }

    @Test
    public void shouldRejectOnMissingPort()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse("192.168.1.20"));
    }

    @Test
    public void shouldRejectOnEmptyPort()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse("192.168.1.20:"));
    }

    @Test
    public void shouldRejectOnEmptyIpV6Port()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse("[::1]:"));
    }

    @Test
    public void shouldRejectOnInvalidIpV6()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse("[FG07::789:1:0:0:3]:111"));
    }

    @Test
    public void shouldRejectOnInvalidIpV6Scope()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse("[FC07::789:1:0:0:3%^]:111"));
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
        final InetSocketAddress address = SocketAddressParser.parse("[::1%12~_.-34]:1234");
        assertThat(address.getAddress(), instanceOf(Inet6Address.class));
    }

    private void assertCorrectParse(final String host, final int port) throws UnknownHostException
    {
        final InetSocketAddress address = SocketAddressParser.parse(host + ":" + port);
        assertEquals(InetAddress.getByName(host), address.getAddress());
        assertEquals(port, address.getPort());
    }

    private void assertCorrectParseIpV6(final String host, final int port) throws UnknownHostException
    {
        final InetSocketAddress address = SocketAddressParser.parse("[" + host + "]:" + port);
        assertEquals(InetAddress.getByName(host), address.getAddress());
        assertEquals(port, address.getPort());
    }
}

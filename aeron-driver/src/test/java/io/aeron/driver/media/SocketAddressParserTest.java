/*
 * Copyright 2014-2021 Real Logic Limited.
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

import io.aeron.driver.DefaultNameResolver;
import io.aeron.driver.NameResolver;
import org.junit.jupiter.api.Test;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SocketAddressParserTest
{
    static final DefaultNameResolver DEFAULT_RESOLVER = DefaultNameResolver.INSTANCE;

    static final String LOOKUP_RESOLVER_NAME = "StreamX";
    static final String LOOKUP_RESOLVER_HOSTNAME = "192.168.1.20";
    static final int LOOKUP_RESOLVER_PORT = 55;
    static final NameResolver LOOKUP_RESOLVER = new NameResolver()
    {
        public InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution)
        {
            return DEFAULT_RESOLVER.resolve(name, uriParamName, isReResolution);
        }

        public String lookup(final String name, final String uriParamName, final boolean isReLookup)
        {
            if (name.equals(LOOKUP_RESOLVER_NAME))
            {
                return LOOKUP_RESOLVER_HOSTNAME + ":" + LOOKUP_RESOLVER_PORT;
            }

            return name;
        }
    };

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
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse(
            "192.168.1.20:aa", ENDPOINT_PARAM_NAME, false, DEFAULT_RESOLVER));
    }

    @Test
    public void shouldRejectOnInvalidPort2()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse(
            "192.168.1.20::123", ENDPOINT_PARAM_NAME, false, DEFAULT_RESOLVER));
    }

    @Test
    public void shouldRejectOnMissingPort()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse(
            "192.168.1.20", ENDPOINT_PARAM_NAME, false, DEFAULT_RESOLVER));
    }

    @Test
    public void shouldRejectOnEmptyPort()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse(
            "192.168.1.20:", ENDPOINT_PARAM_NAME, false, DEFAULT_RESOLVER));
    }

    @Test
    public void shouldRejectOnEmptyIpV6Port()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse(
            "[::1]:", ENDPOINT_PARAM_NAME, false, DEFAULT_RESOLVER));
    }

    @Test
    public void shouldRejectOnInvalidIpV6()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse(
            "[FG07::789:1:0:0:3]:111", ENDPOINT_PARAM_NAME, false, DEFAULT_RESOLVER));
    }

    @Test
    public void shouldRejectOnInvalidIpV6Scope()
    {
        assertThrows(IllegalArgumentException.class, () -> SocketAddressParser.parse(
            "[FC07::789:1:0:0:3%^]:111", ENDPOINT_PARAM_NAME, false, DEFAULT_RESOLVER));
    }

    @Test
    public void shouldParseIpV6() throws Exception
    {
        assertCorrectParseIpV6("::1", 54321);
        assertCorrectParseIpV6("FC07::789:1:0:0:3", 54321);
        assertCorrectParseIpV6("fc07::789:1:0:0:3", 54321);
    }

    @Test
    public void shouldParseWithScope() throws UnknownHostException
    {
        final InetSocketAddress address = SocketAddressParser.parse(
            "[::1%12~_.-34]:1234", ENDPOINT_PARAM_NAME, false, DEFAULT_RESOLVER);
        assertThat(address.getAddress(), instanceOf(Inet6Address.class));
    }

    @Test
    public void shouldParseAndLookupResolverForName() throws UnknownHostException
    {
        final InetSocketAddress socketAddress = SocketAddressParser.parse(
            LOOKUP_RESOLVER_NAME, ENDPOINT_PARAM_NAME, false, LOOKUP_RESOLVER);
        assertEquals(InetAddress.getByName(LOOKUP_RESOLVER_HOSTNAME), socketAddress.getAddress());
        assertEquals(LOOKUP_RESOLVER_PORT, socketAddress.getPort());
    }

    @Test
    public void shouldParseAndPassThroughLookupForUnknownName() throws UnknownHostException
    {
        final InetSocketAddress socketAddress = SocketAddressParser.parse(
            LOOKUP_RESOLVER_HOSTNAME + ":" + LOOKUP_RESOLVER_PORT, ENDPOINT_PARAM_NAME, false, LOOKUP_RESOLVER);
        assertEquals(InetAddress.getByName(LOOKUP_RESOLVER_HOSTNAME), socketAddress.getAddress());
        assertEquals(LOOKUP_RESOLVER_PORT, socketAddress.getPort());
    }

    private void assertCorrectParse(final String host, final int port) throws UnknownHostException
    {
        final InetSocketAddress address = SocketAddressParser.parse(
            host + ":" + port, ENDPOINT_PARAM_NAME, false, DEFAULT_RESOLVER);
        assertEquals(InetAddress.getByName(host), address.getAddress());
        assertEquals(port, address.getPort());
    }

    private void assertCorrectParseIpV6(final String host, final int port) throws UnknownHostException
    {
        final InetSocketAddress address = SocketAddressParser.parse(
            "[" + host + "]:" + port, ENDPOINT_PARAM_NAME, false, DEFAULT_RESOLVER);
        assertEquals(InetAddress.getByName(host), address.getAddress());
        assertEquals(port, address.getPort());
    }
}

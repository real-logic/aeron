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
package io.aeron.driver;

import io.aeron.driver.exceptions.InvalidChannelException;
import io.aeron.driver.media.UdpChannel;
import org.agrona.BitUtil;
import org.agrona.LangUtil;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.*;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class UdpChannelTest
{
    @Test
    public void shouldHandleExplicitLocalAddressAndPortFormat()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?interface=localhost:40123|endpoint=localhost:40124");

        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test
    public void shouldNotAllowDynamicControlModeWithoutExplicitControl()
    {
        try
        {
            UdpChannel.parse("aeron:udp?control-mode=dynamic");
            fail("InvalidChannelException expected");
        }
        catch (final InvalidChannelException ex)
        {
            final Throwable cause = ex.getCause();
            assertNotNull(cause);
            assertThat(cause.getMessage(), containsString("explicit control expected with dynamic control mode"));
        }
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    public void shouldHandleExplicitLocalAddressAndPortFormatWithAeronUri(
        final String endpointKey,
        final String interfaceKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(
            uri(endpointKey, "localhost:40124", interfaceKey, "localhost:40123"));

        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test
    public void shouldHandleImpliedLocalAddressAndPortFormat()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=localhost:40124");

        assertThat(udpChannel.localData(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    public void shouldHandleImpliedLocalAddressAndPortFormatWithAeronUri(
        final String endpointKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(uri(endpointKey, "localhost:40124"));

        assertThat(udpChannel.localData(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test
    public void shouldThrowExceptionForIncorrectScheme()
    {
        assertThrows(InvalidChannelException.class, () -> UdpChannel.parse("unknownudp://localhost:40124"));
    }

    @Test
    public void shouldThrowExceptionForMissingAddressWithAeronUri()
    {
        assertThrows(InvalidChannelException.class, () -> UdpChannel.parse("aeron:udp"));
    }

    @Test
    public void shouldThrowExceptionOnEvenMulticastAddress()
    {
        assertThrows(InvalidChannelException.class, () -> UdpChannel.parse("aeron:udp?endpoint=224.10.9.8"));
    }

    @Test
    public void shouldParseValidMulticastAddress() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?interface=localhost|endpoint=224.10.9.9:40124");

        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteControl(), isMulticastAddress("224.10.9.10", 40124));
        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteData(), isMulticastAddress("224.10.9.9", 40124));
        assertThat(udpChannel.localInterface(),
            is(NetworkInterface.getByInetAddress(InetAddress.getByName("localhost"))));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    public void shouldParseValidMulticastAddressWithAeronUri(
        final String endpointKey,
        final String interfaceKey)
        throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse(
            uri(endpointKey, "224.10.9.9:40124", interfaceKey, "localhost"));

        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteControl(), isMulticastAddress("224.10.9.10", 40124));
        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteData(), isMulticastAddress("224.10.9.9", 40124));
        assertThat(udpChannel.localInterface(),
            is(NetworkInterface.getByInetAddress(InetAddress.getByName("localhost"))));
    }

    private Matcher<InetSocketAddress> isMulticastAddress(final String addressName, final int port)
        throws UnknownHostException
    {
        final InetAddress inetAddress = InetAddress.getByName(addressName);
        return is(new InetSocketAddress(inetAddress, port));
    }

    @Test
    public void shouldHandleImpliedLocalPortFormat()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?interface=localhost|endpoint=localhost:40124");

        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    public void shouldHandleImpliedLocalPortFormatWithAeronUri(
        final String endpointKey,
        final String interfaceKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(
            uri(endpointKey, "localhost:40124", interfaceKey, "localhost"));

        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    public void shouldHandleIPv4AnyAddressAsInterfaceAddressForUnicast(
        final String endpointKey,
        final String interfaceKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(
            uri(endpointKey, "localhost:40124", interfaceKey, "0.0.0.0"));

        assertThat(udpChannel.localData(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    public void shouldHandleIPv6AnyAddressAsInterfaceAddressForUnicast(
        final String endpointKey,
        final String interfaceKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(uri(endpointKey, "[::1]:40124", interfaceKey, "[::]"));

        assertThat(udpChannel.localData(), is(new InetSocketAddress("::", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("::", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("::1", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("::1", 40124)));
    }

    @Test
    public void shouldHandleLocalhostLookup()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=localhost:40124");

        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("127.0.0.1", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("127.0.0.1", 40124)));
    }

    @ParameterizedTest
    @ValueSource(strings = "endpoint")
    public void shouldHandleLocalhostLookupWithAeronUri(final String endpointKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(uri(endpointKey, "localhost:40124"));

        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("127.0.0.1", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("127.0.0.1", 40124)));
    }

    @Test
    public void shouldHandleBeingUsedAsMapKey()
    {
        final UdpChannel udpChannel1 = UdpChannel.parse("aeron:udp?endpoint=localhost:40124");
        final UdpChannel udpChannel2 = UdpChannel.parse("aeron:udp?endpoint=localhost:40124");

        final Map<UdpChannel, Integer> map = new HashMap<>();

        map.put(udpChannel1, 1);
        assertThat(map.get(udpChannel2), is(1));
    }

    @Test
    public void shouldThrowExceptionWhenNoPortSpecified()
    {
        assertThrows(InvalidChannelException.class, () -> UdpChannel.parse("aeron:udp?endpoint=localhost"));
    }

    @Test
    public void shouldHandleCanonicalFormForUnicastCorrectly()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=192.168.0.1:40456");
        final UdpChannel udpChannelLocal = UdpChannel.parse("aeron:udp?interface=127.0.0.1|endpoint=192.168.0.1:40456");
        final UdpChannel udpChannelLocalPort = UdpChannel.parse(
            "aeron:udp?interface=127.0.0.1:40455|endpoint=192.168.0.1:40456");
        final UdpChannel udpChannelLocalhost = UdpChannel.parse(
            "aeron:udp?interface=localhost|endpoint=localhost:40456");

        assertThat(udpChannel.canonicalForm(), is("UDP-00000000-0-c0a80001-40456"));
        assertThat(udpChannelLocal.canonicalForm(), is("UDP-7f000001-0-c0a80001-40456"));
        assertThat(udpChannelLocalPort.canonicalForm(), is("UDP-7f000001-40455-c0a80001-40456"));
        assertThat(udpChannelLocalhost.canonicalForm(), is("UDP-7f000001-0-7f000001-40456"));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    public void shouldHandleIpV6CanonicalFormForUnicastCorrectly(
        final String endpointKey,
        final String interfaceKey)
    {
        final UdpChannel udpChannelLocal =
            UdpChannel.parse(uri(endpointKey, "192.168.0.1:40456", interfaceKey, "[::1]"));
        final UdpChannel udpChannelLocalPort =
            UdpChannel.parse(uri(endpointKey, "[fe80::5246:5dff:fe73:df06]:40456", interfaceKey, "127.0.0.1:40455"));

        assertThat(udpChannelLocal.canonicalForm(), is("UDP-00000000000000000000000000000001-0-c0a80001-40456"));
        assertThat(udpChannelLocalPort.canonicalForm(),
            is("UDP-7f000001-40455-fe8000000000000052465dfffe73df06-40456"));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    public void shouldHandleCanonicalFormForUnicastCorrectlyWithAeronUri(
        final String endpointKey,
        final String interfaceKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(uri(endpointKey, "192.168.0.1:40456"));
        final UdpChannel udpChannelLocal =
            UdpChannel.parse(uri(endpointKey, "192.168.0.1:40456", interfaceKey, "127.0.0.1"));
        final UdpChannel udpChannelLocalPort =
            UdpChannel.parse(uri(endpointKey, "192.168.0.1:40456", interfaceKey, "127.0.0.1:40455"));
        final UdpChannel udpChannelLocalhost =
            UdpChannel.parse(uri(endpointKey, "localhost:40456", interfaceKey, "localhost"));

        assertThat(udpChannel.canonicalForm(), is("UDP-00000000-0-c0a80001-40456"));
        assertThat(udpChannelLocal.canonicalForm(), is("UDP-7f000001-0-c0a80001-40456"));
        assertThat(udpChannelLocalPort.canonicalForm(), is("UDP-7f000001-40455-c0a80001-40456"));
        assertThat(udpChannelLocalhost.canonicalForm(), is("UDP-7f000001-0-7f000001-40456"));
    }

    @Test
    public void shouldGetProtocolFamilyForIpV4()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:12345|interface=127.0.0.1");
        assertThat(udpChannel.protocolFamily(), is(StandardProtocolFamily.INET));
    }

    @Test
    public void shouldGetProtocolFamilyForIpV6()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=[::1]:12345|interface=[::1]");
        assertThat(udpChannel.protocolFamily(), is(StandardProtocolFamily.INET6));
    }

    @Test
    public void shouldGetProtocolFamilyForIpV4WithoutLocalSpecified()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:12345");
        assertThat(udpChannel.protocolFamily(), is(StandardProtocolFamily.INET));
    }

    @Test
    public void shouldGetProtocolFamilyForIpV6WithoutLocalSpecified()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=[::1]:12345");
        assertThat(udpChannel.protocolFamily(), is(StandardProtocolFamily.INET6));
    }

    @Test
    public void shouldHandleCanonicalFormWithNsLookup() throws Exception
    {
        final String localhostIpAsHex = resolveToHexAddress("localhost");

        final UdpChannel udpChannelExampleCom = UdpChannel.parse("aeron:udp?endpoint=localhost:40456");
        assertThat(udpChannelExampleCom.canonicalForm(), is("UDP-00000000-0-" + localhostIpAsHex + "-40456"));
    }

    @Test
    public void shouldHandleCanonicalFormForMulticastWithLocalPort()
    {
        final UdpChannel udpChannelLocalPort = UdpChannel.parse(
            "aeron:udp?interface=127.0.0.1:40455|endpoint=224.0.1.1:40456");
        assertThat(udpChannelLocalPort.canonicalForm(), is("UDP-7f000001-40455-e0000101-40456"));

        final UdpChannel udpChannelSubnetLocalPort =
            UdpChannel.parse("aeron:udp?interface=127.0.0.0:40455/24|endpoint=224.0.1.1:40456");
        assertThat(udpChannelSubnetLocalPort.canonicalForm(), is("UDP-7f000001-40455-e0000101-40456"));
    }

    @Test
    public void shouldHandleCanonicalFormForMulticastCorrectly()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?interface=localhost|endpoint=224.0.1.1:40456");
        final UdpChannel udpChannelLocal = UdpChannel.parse("aeron:udp?interface=127.0.0.1|endpoint=224.0.1.1:40456");
        final UdpChannel udpChannelAllSystems = UdpChannel.parse(
            "aeron:udp?interface=localhost|endpoint=224.0.0.1:40456");
        final UdpChannel udpChannelDefault = UdpChannel.parse("aeron:udp?endpoint=224.0.1.1:40456");

        final UdpChannel udpChannelSubnet = UdpChannel.parse(
            "aeron:udp?interface=localhost/24|endpoint=224.0.1.1:40456");
        final UdpChannel udpChannelSubnetLocal = UdpChannel.parse(
            "aeron:udp?interface=127.0.0.0/24|endpoint=224.0.1.1:40456");

        assertThat(udpChannel.canonicalForm(), is("UDP-7f000001-0-e0000101-40456"));
        assertThat(udpChannelLocal.canonicalForm(), is("UDP-7f000001-0-e0000101-40456"));
        assertThat(udpChannelAllSystems.canonicalForm(), is("UDP-7f000001-0-e0000001-40456"));
        assertThat(udpChannelSubnet.canonicalForm(), is("UDP-7f000001-0-e0000101-40456"));
        assertThat(udpChannelSubnetLocal.canonicalForm(), is("UDP-7f000001-0-e0000101-40456"));

        assertThat(udpChannelDefault.localInterface(), supportsMulticastOrIsLoopback());
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    public void shouldHandleCanonicalFormForMulticastCorrectlyWithAeronUri(
        final String endpointKey,
        final String interfaceKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(uri(endpointKey, "224.0.1.1:40456", interfaceKey, "localhost"));
        final UdpChannel udpChannelLocal =
            UdpChannel.parse(uri(endpointKey, "224.0.1.1:40456", interfaceKey, "127.0.0.1"));
        final UdpChannel udpChannelAllSystems =
            UdpChannel.parse(uri(endpointKey, "224.0.0.1:40456", interfaceKey, "127.0.0.1"));
        final UdpChannel udpChannelDefault = UdpChannel.parse(uri(endpointKey, "224.0.1.1:40456"));
        final UdpChannel udpChannelSubnet =
            UdpChannel.parse(uri(endpointKey, "224.0.1.1:40456", interfaceKey, "localhost/24"));
        final UdpChannel udpChannelSubnetLocal =
            UdpChannel.parse(uri(endpointKey, "224.0.1.1:40456", interfaceKey, "127.0.0.0/24"));

        assertThat(udpChannel.canonicalForm(), is("UDP-7f000001-0-e0000101-40456"));
        assertThat(udpChannelLocal.canonicalForm(), is("UDP-7f000001-0-e0000101-40456"));
        assertThat(udpChannelAllSystems.canonicalForm(), is("UDP-7f000001-0-e0000001-40456"));
        assertThat(udpChannelSubnet.canonicalForm(), is("UDP-7f000001-0-e0000101-40456"));
        assertThat(udpChannelSubnetLocal.canonicalForm(), is("UDP-7f000001-0-e0000101-40456"));
        assertThat(udpChannelDefault.localInterface(), supportsMulticastOrIsLoopback());
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    public void shouldHandleIpV6CanonicalFormForMulticastCorrectly(
        final String endpointKey,
        final String interfaceKey)
    {
        assumeTrue(System.getProperty("java.net.preferIPv4Stack") == null);

        final UdpChannel udpChannel =
            UdpChannel.parse(uri(endpointKey, "[FF01::FD]:40456", interfaceKey, "localhost"));
        final UdpChannel udpChannelLocal =
            UdpChannel.parse(uri(endpointKey, "224.0.1.1:40456", interfaceKey, "[::1]:54321/64"));

        assertThat(udpChannel.canonicalForm(), is("UDP-7f000001-0-ff0100000000000000000000000000fd-40456"));
        assertThat(udpChannelLocal.canonicalForm(), is("UDP-00000000000000000000000000000001-54321-e0000101-40456"));
    }

    private static Matcher<NetworkInterface> supportsMulticastOrIsLoopback()
    {
        return new NetworkInterfaceTypeSafeMatcher();
    }

    private String resolveToHexAddress(final String host) throws UnknownHostException
    {
        return BitUtil.toHex(InetAddress.getByName(host).getAddress());
    }

    private static String uri(
        final String endpointKey, final String endpointValue, final String interfaceKey, final String interfaceValue)
    {
        return "aeron:udp?" + endpointKey + "=" + endpointValue + "|" + interfaceKey + "=" + interfaceValue;
    }

    private static String uri(final String endpointKey, final String endpointValue)
    {
        return "aeron:udp?" + endpointKey + "=" + endpointValue;
    }

    static class NetworkInterfaceTypeSafeMatcher extends TypeSafeMatcher<NetworkInterface>
    {
        public void describeTo(final Description description)
        {
            description.appendText("Interface supports multicast or is loopack");
        }

        protected boolean matchesSafely(final NetworkInterface item)
        {
            boolean matchesSafely = false;
            try
            {
                matchesSafely = item.supportsMulticast() || item.isLoopback();
            }
            catch (final SocketException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return matchesSafely;
        }
    }
}

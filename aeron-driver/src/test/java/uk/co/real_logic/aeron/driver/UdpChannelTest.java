/*
 * Copyright 2014 Real Logic Ltd.
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

package uk.co.real_logic.aeron.driver;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.hamcrest.Matcher;
import org.junit.Test;

import uk.co.real_logic.aeron.driver.exceptions.InvalidChannelException;

public class UdpChannelTest
{
    @Test
    public void shouldHandleExplicitLocalAddressAndPortFormat() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse("udp://localhost:40123@localhost:40124");

        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test
    public void shouldHandleImpliedLocalAddressAndPortFormat() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse("udp://localhost:40124");

        assertThat(udpChannel.localData(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test(expected = InvalidChannelException.class)
    public void shouldThrowExceptionForIncorrectScheme() throws Exception
    {
        UdpChannel.parse("unknownudp://localhost:40124");
    }

    @Test(expected = InvalidChannelException.class)
    public void shouldThrowExceptionForMissingAddress() throws Exception
    {
        UdpChannel.parse("udp://");
    }

    @Test(expected = InvalidChannelException.class)
    public void shouldThrowExceptionOnEvenMulticastAddress() throws Exception
    {
        UdpChannel.parse("udp://224.10.9.8");
    }

    @Test
    public void shouldParseValidMulticastAddress() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse("udp://localhost@224.10.9.9:40124");

        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteControl(), isMulticastAddress("224.10.9.10", 40124));
        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteData(), isMulticastAddress("224.10.9.9", 40124));
        assertThat(udpChannel.localInterface(), is(NetworkInterface.getByInetAddress(InetAddress.getByName("localhost"))));
    }

    private Matcher<InetSocketAddress> isMulticastAddress(String addressName, int port) throws UnknownHostException
    {
        final InetAddress inetAddress = InetAddress.getByName(addressName);
        return is(new InetSocketAddress(inetAddress, port));
    }

    @Test
    public void shouldHandleImpliedLocalPortFormat() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse("udp://localhost@localhost:40124");

        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test
    public void shouldHandleLocalhostLookup() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse("udp://localhost:40124");

        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("127.0.0.1", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("127.0.0.1", 40124)));
    }

    @Test
    public void shouldHandleBeingUsedAsMapKey() throws Exception
    {
        final UdpChannel udpChannel1 = UdpChannel.parse("udp://localhost:40124");
        final UdpChannel udpChannel2 = UdpChannel.parse("udp://localhost:40124");

        final Map<UdpChannel, Integer> map = new HashMap<>();

        map.put(udpChannel1, 1);
        assertThat(map.get(udpChannel2), is(1));
    }

    @Test(expected =  InvalidChannelException.class)
    public void shouldThrowExceptionWhenNoPortSpecified() throws Exception
    {
        UdpChannel.parse("udp://localhost");
    }

    @Test
    public void shouldHandleCanonicalFormForUnicastCorrectly() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse("udp://192.168.0.1:40456");
        final UdpChannel udpChannelLocal = UdpChannel.parse("udp://127.0.0.1@192.168.0.1:40456");
        final UdpChannel udpChannelLocalPort = UdpChannel.parse("udp://127.0.0.1:40455@192.168.0.1:40456");
        final UdpChannel udpChannelLocalhost = UdpChannel.parse("udp://localhost@localhost:40456");
        // should resolve to 93.184.216.119
        final UdpChannel udpChannelExampleCom = UdpChannel.parse("udp://example.com:40456");

        assertThat(udpChannel.canonicalForm(), is("UDP-00000000-0-c0a80001-40456"));
        assertThat(udpChannelLocal.canonicalForm(), is("UDP-7f000001-0-c0a80001-40456"));
        assertThat(udpChannelLocalPort.canonicalForm(), is("UDP-7f000001-40455-c0a80001-40456"));
        assertThat(udpChannelLocalhost.canonicalForm(), is("UDP-7f000001-0-7f000001-40456"));
        assertThat(udpChannelExampleCom.canonicalForm(), is("UDP-00000000-0-5db8d877-40456"));
    }

    @Test
    public void shouldHandleCanonicalFormForMulticastCorrectly() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse("udp://localhost@224.0.1.1:40456");
        final UdpChannel udpChannelLocal = UdpChannel.parse("udp://127.0.0.1@224.0.1.1:40456");
        final UdpChannel udpChannelLocalPort = UdpChannel.parse("udp://127.0.0.1:40455@224.0.1.1:40456");
        final UdpChannel udpChannelAllSystems = UdpChannel.parse("udp://localhost@all-systems.mcast.net:40456");

        assertThat(udpChannel.canonicalForm(), is("UDP-7f000001-0-e0000101-40456"));
        assertThat(udpChannelLocal.canonicalForm(), is("UDP-7f000001-0-e0000101-40456"));
        assertThat(udpChannelLocalPort.canonicalForm(), is("UDP-7f000001-40455-e0000101-40456"));
        assertThat(udpChannelAllSystems.canonicalForm(), is("UDP-7f000001-0-e0000001-40456"));
    }

    @Test
//    @Ignore
    public void shouldHandleCanonicalFormForMulticastWithMaskCorrectly() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse("udp://172.29.1.0@224.0.1.1:40456?mask=24");
//        final UdpChannel udpChannelLocal = UdpChannel.parse("udp://127.0.0.1@224.0.1.1:40456");
//        final UdpChannel udpChannelLocalPort = UdpChannel.parse("udp://127.0.0.1:40455@224.0.1.1:40456");
//        final UdpChannel udpChannelAllSystems = UdpChannel.parse("udp://localhost@all-systems.mcast.net:40456");
//
//        assertThat(udpChannel.canonicalForm(), is("UDP-7f000001-0-e0000101-40456"));
//        assertThat(udpChannelLocal.canonicalForm(), is("UDP-7f000001-0-e0000101-40456"));
//        assertThat(udpChannelLocalPort.canonicalForm(), is("UDP-7f000001-40455-e0000101-40456"));
//        assertThat(udpChannelAllSystems.canonicalForm(), is("UDP-7f000001-0-e0000001-40456"));
    }
}

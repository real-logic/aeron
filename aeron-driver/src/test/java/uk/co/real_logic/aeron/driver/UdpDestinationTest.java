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

import org.hamcrest.Matcher;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class UdpDestinationTest
{
    @Test
    public void shouldHandleExplicitLocalAddressAndPortFormat() throws Exception
    {
        final UdpDestination dst = UdpDestination.parse("udp://localhost:40123@localhost:40124");

        assertThat(dst.localData(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(dst.localControl(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(dst.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(dst.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test
    public void shouldHandleImpliedLocalAddressAndPortFormat() throws Exception
    {
        final UdpDestination dst = UdpDestination.parse("udp://localhost:40124");

        assertThat(dst.localData(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(dst.localControl(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(dst.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(dst.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test(expected = InvalidDestinationException.class)
    public void shouldThrowExceptionForIncorrectScheme() throws Exception
    {
        UdpDestination.parse("unknwonudp://localhost:40124");
    }

    @Test(expected = InvalidDestinationException.class)
    public void shouldThrowExceptionForMissingAddress() throws Exception
    {
        UdpDestination.parse("udp://");
    }

    @Test(expected = InvalidDestinationException.class)
    public void shouldThrowExceptionOnEvenMulticastAddress() throws Exception
    {
        UdpDestination.parse("udp://224.10.9.8");
    }

    @Test
    public void shouldParseValidMulticastAddress() throws Exception
    {
        final UdpDestination dst = UdpDestination.parse("udp://localhost@224.10.9.9:40124");

        assertThat(dst.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(dst.remoteControl(), isMulticastAddress("224.10.9.10", 40124));
        assertThat(dst.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(dst.remoteData(), isMulticastAddress("224.10.9.9", 40124));
        assertThat(dst.localInterface(), is(NetworkInterface.getByInetAddress(InetAddress.getByName("localhost"))));
    }

    private Matcher<InetSocketAddress> isMulticastAddress(String addressName, int port) throws UnknownHostException
    {
        final InetAddress inetAddress = InetAddress.getByName(addressName);
        return is(new InetSocketAddress(inetAddress, port));
    }

    @Test
    public void shouldHandleImpliedLocalPortFormat() throws Exception
    {
        final UdpDestination dst = UdpDestination.parse("udp://localhost@localhost:40124");

        assertThat(dst.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(dst.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(dst.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(dst.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test
    public void shouldHandleLocalhostLookup() throws Exception
    {
        final UdpDestination dst = UdpDestination.parse("udp://localhost:40124");

        assertThat(dst.remoteData(), is(new InetSocketAddress("127.0.0.1", 40124)));
        assertThat(dst.remoteControl(), is(new InetSocketAddress("127.0.0.1", 40124)));
    }

    @Test
    public void shouldHandleBeingUsedAsMapKey() throws Exception
    {
        final UdpDestination dst1 = UdpDestination.parse("udp://localhost:40124");
        final UdpDestination dst2 = UdpDestination.parse("udp://localhost:40124");

        final Map<UdpDestination, Integer> map = new HashMap<>();

        map.put(dst1, 1);
        assertThat(map.get(dst2), is(1));
    }

    @Test(expected =  InvalidDestinationException.class)
    public void shouldThrowExceptionWhenNoDestinationPortSpecified() throws Exception
    {
        UdpDestination.parse("udp://localhost");
    }

    @Test
    public void shouldHandleConsistentHashCorrectly() throws Exception
    {
        final UdpDestination dst1 = UdpDestination.parse("udp://localhost:40124");
        final UdpDestination dst2 = UdpDestination.parse("udp://localhost:40124");
        final UdpDestination dst3 = UdpDestination.parse("udp://localhost:40123");

        assertThat(dst1.consistentHash(), is(dst2.consistentHash()));
        assertThat(dst2.consistentHash(), not(dst3.consistentHash()));
    }

    @Test
    public void shouldHandleCanonicalRepresentationForUnicastCorrectly() throws Exception
    {
        final UdpDestination dst = UdpDestination.parse("udp://192.168.0.1:40456");
        final UdpDestination dstLocal = UdpDestination.parse("udp://127.0.0.1@192.168.0.1:40456");
        final UdpDestination dstLocalPort = UdpDestination.parse("udp://127.0.0.1:40455@192.168.0.1:40456");
        final UdpDestination dstLocalhost = UdpDestination.parse("udp://localhost@localhost:40456");
        // should resolve to 93.184.216.119
        final UdpDestination dstExampleCom = UdpDestination.parse("udp://example.com:40456");

        assertThat(dst.canonicalRepresentation(), is("UDP-00000000-0-c0a80001-40456"));
        assertThat(dstLocal.canonicalRepresentation(), is("UDP-7f000001-0-c0a80001-40456"));
        assertThat(dstLocalPort.canonicalRepresentation(), is("UDP-7f000001-40455-c0a80001-40456"));
        assertThat(dstLocalhost.canonicalRepresentation(), is("UDP-7f000001-0-7f000001-40456"));
        assertThat(dstExampleCom.canonicalRepresentation(), is("UDP-00000000-0-5db8d877-40456"));
    }

    @Test
    public void shouldHandleCanonicalRepresentationForMulticastCorrectly() throws Exception
    {
        final UdpDestination dst = UdpDestination.parse("udp://localhost@224.0.1.1:40456");
        final UdpDestination dstLocal = UdpDestination.parse("udp://127.0.0.1@224.0.1.1:40456");
        final UdpDestination dstLocalPort = UdpDestination.parse("udp://127.0.0.1:40455@224.0.1.1:40456");
        final UdpDestination dstAllSystems = UdpDestination.parse("udp://localhost@all-systems.mcast.net:40456");

        assertThat(dst.canonicalRepresentation(), is("UDP-7f000001-0-e0000101-40456"));
        assertThat(dstLocal.canonicalRepresentation(), is("UDP-7f000001-0-e0000101-40456"));
        assertThat(dstLocalPort.canonicalRepresentation(), is("UDP-7f000001-40455-e0000101-40456"));
        assertThat(dstAllSystems.canonicalRepresentation(), is("UDP-7f000001-0-e0000001-40456"));
    }
}

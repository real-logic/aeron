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
package uk.co.real_logic.aeron.mediadriver;

import org.hamcrest.Matcher;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class UdpDestinationTest
{
    @Test
    public void shouldHandleExplicitLocalAddrAndPortFormat() throws Exception
    {
        final UdpDestination dest = UdpDestination.parse("udp://localhost:40123@localhost:40124");

        assertThat(dest.localData(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(dest.localControl(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(dest.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(dest.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test
    public void shouldHandleImpliedLocalAddrAndPortFormat() throws Exception
    {
        final UdpDestination dest = UdpDestination.parse("udp://localhost:40124");

        assertThat(dest.localData(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(dest.localControl(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(dest.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(dest.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
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
        final UdpDestination dest = UdpDestination.parse("udp://224.10.9.9:40124");

        assertThat(dest.localControl(), isMulticastAddress("224.10.9.10", 40124));
        assertThat(dest.remoteControl(), isMulticastAddress("224.10.9.10", 40124));
        assertThat(dest.localData(), isMulticastAddress("224.10.9.9", 40124));
        assertThat(dest.remoteData(), isMulticastAddress("224.10.9.9", 40124));
    }

    private Matcher<InetSocketAddress> isMulticastAddress(String addressName, int port) throws UnknownHostException
    {
        final InetAddress inetAddress = InetAddress.getByName(addressName);
        return is(new InetSocketAddress(inetAddress, port));
    }

    @Test
    public void shouldHandleImpliedLocalPortFormat() throws Exception
    {
        final UdpDestination dest = UdpDestination.parse("udp://localhost@localhost:40124");

        assertThat(dest.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(dest.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(dest.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(dest.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test
    public void shouldHandleLocalhostLookup() throws Exception
    {
        final UdpDestination dest = UdpDestination.parse("udp://localhost:40124");

        assertThat(dest.remoteData(), is(new InetSocketAddress("127.0.0.1", 40124)));
        assertThat(dest.remoteControl(), is(new InetSocketAddress("127.0.0.1", 40124)));
    }

    @Test
    public void shouldHandleBeingUsedAsMapKey() throws Exception
    {
        final UdpDestination dest1 = UdpDestination.parse("udp://localhost:40124");
        final UdpDestination dest2 = UdpDestination.parse("udp://localhost:40124");

        final Map<UdpDestination, Integer> map = new HashMap<>();

        map.put(dest1, 1);
        assertThat(map.get(dest2), is(1));
    }

    @Test(expected =  InvalidDestinationException.class)
    public void shouldThrowExceptionWhenNoDestinationPortSpecified() throws Exception
    {
        UdpDestination.parse("udp://localhost");
    }

    @Test
    public void shouldHandleConsistentHashCorrectly() throws Exception
    {
        final UdpDestination dest1 = UdpDestination.parse("udp://localhost:40124");
        final UdpDestination dest2 = UdpDestination.parse("udp://localhost:40124");
        final UdpDestination dest3 = UdpDestination.parse("udp://localhost:40123");

        assertThat(dest1.consistentHash(), is(dest2.consistentHash()));
        assertThat(dest2.consistentHash(), not(dest3.consistentHash()));
    }

    @Test
    public void shouldHandleCanonicalRepresentationForUnicastCorrectly() throws Exception
    {
        final UdpDestination dest = UdpDestination.parse("udp://192.168.0.1:40456");
        final UdpDestination destLocal = UdpDestination.parse("udp://127.0.0.1@192.168.0.1:40456");
        final UdpDestination destLocalPort = UdpDestination.parse("udp://127.0.0.1:40455@192.168.0.1:40456");
        final UdpDestination destLocalhost = UdpDestination.parse("udp://localhost@localhost:40456");
        // should resolve to 93.184.216.119
        final UdpDestination destExampleCom = UdpDestination.parse("udp://example.com:40456");

        assertThat(dest.canonicalRepresentation(), is("UDP-00000000-0-c0a80001-40456"));
        assertThat(destLocal.canonicalRepresentation(), is("UDP-7f000001-0-c0a80001-40456"));
        assertThat(destLocalPort.canonicalRepresentation(), is("UDP-7f000001-40455-c0a80001-40456"));
        assertThat(destLocalhost.canonicalRepresentation(), is("UDP-7f000001-0-7f000001-40456"));
        assertThat(destExampleCom.canonicalRepresentation(), is("UDP-00000000-0-5db8d877-40456"));
    }

    @Test
    public void shouldHandleCanonicalRepresentationForMulticastCorrectly() throws Exception
    {
        final UdpDestination dest = UdpDestination.parse("udp://224.0.1.1:40456");
        final UdpDestination destLocal = UdpDestination.parse("udp://127.0.0.1@224.0.1.1:40456");
        final UdpDestination destLocalPort = UdpDestination.parse("udp://127.0.0.1:40455@224.0.1.1:40456");
        final UdpDestination destAllSystems = UdpDestination.parse("udp://all-systems.mcast.net:40456");

        assertThat(dest.canonicalRepresentation(), is("UDP-00000000-0-e0000101-40456"));
        assertThat(destLocal.canonicalRepresentation(), is("UDP-7f000001-0-e0000101-40456"));
        assertThat(destLocalPort.canonicalRepresentation(), is("UDP-7f000001-40455-e0000101-40456"));
        assertThat(destAllSystems.canonicalRepresentation(), is("UDP-00000000-0-e0000001-40456"));
    }

}

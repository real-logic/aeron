/*
 * Copyright 2014-2025 Real Logic Limited.
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

import java.net.BindException;
import java.net.InetSocketAddress;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WildcardPortManagerTest
{
    final int[] portRange = new int[]{ 20000, 20003};
    final UdpChannel udpChannelPort0 = UdpChannel.parse("aeron:udp?endpoint=localhost:0");
    final UdpChannel udpChannelPubControl = UdpChannel.parse("aeron:udp?control=localhost:0|endpoint=localhost:9999");

    @Test
    void shouldAllocateConsecutivePortsInRange() throws BindException
    {
        final WildcardPortManager manager = new WildcardPortManager(portRange, false);
        final InetSocketAddress bindAddress = new InetSocketAddress("localhost", 0);

        assertThat(manager.getManagedPort(
            udpChannelPort0, bindAddress), is(new InetSocketAddress("localhost", 20000)));
        assertThat(manager.getManagedPort(
            udpChannelPort0, bindAddress), is(new InetSocketAddress("localhost", 20001)));
        assertThat(manager.getManagedPort(
            udpChannelPort0, bindAddress), is(new InetSocketAddress("localhost", 20002)));
    }

    @Test
    void shouldPassThrough0WithNullRanges() throws BindException
    {
        final InetSocketAddress bindAddress = new InetSocketAddress("localhost", 0);
        final WildcardPortManager manager = new WildcardPortManager(WildcardPortManager.EMPTY_PORT_RANGE, false);

        assertThat(manager.getManagedPort(
            udpChannelPort0, bindAddress), is(new InetSocketAddress("localhost", 0)));
    }

    @Test
    void shouldPassThrough0WithSenderPubWithoutControlAddress() throws BindException
    {
        final InetSocketAddress bindAddress = new InetSocketAddress("localhost", 0);
        final WildcardPortManager manager = new WildcardPortManager(portRange, true);

        assertThat(manager.getManagedPort(
            udpChannelPort0, bindAddress), is(new InetSocketAddress("localhost", 0)));
    }

    @Test
    void shouldPassThroughWithExplicitBindAddressOutSideRange() throws BindException
    {
        final InetSocketAddress bindAddress = new InetSocketAddress("localhost", 1000);
        final WildcardPortManager manager = new WildcardPortManager(portRange, true);

        assertThat(manager.getManagedPort(
            udpChannelPort0, bindAddress), is(new InetSocketAddress("localhost", 1000)));
    }

    @Test
    void shouldPassThroughWithExplicitBindAddressInsideRange() throws BindException
    {
        final InetSocketAddress bindAddress = new InetSocketAddress("localhost", 20003);
        final WildcardPortManager manager = new WildcardPortManager(portRange, true);

        assertThat(manager.getManagedPort(
            udpChannelPort0, bindAddress), is(new InetSocketAddress("localhost", 20003)));
    }

    @Test
    void shouldAllocateForPubWithExplicitControlAddress() throws BindException
    {
        final InetSocketAddress bindAddress = new InetSocketAddress("localhost", 0);
        final WildcardPortManager manager = new WildcardPortManager(portRange, true);

        assertThat(manager.getManagedPort(
            udpChannelPubControl, bindAddress), is(new InetSocketAddress("localhost", 20000)));
    }

    @Test
    void shouldThrowOnExhaustion() throws BindException
    {
        final InetSocketAddress bindAddress = new InetSocketAddress("localhost", 0);
        final WildcardPortManager manager = new WildcardPortManager(portRange, false);

        manager.getManagedPort(udpChannelPort0, bindAddress);
        manager.getManagedPort(udpChannelPort0, bindAddress);
        manager.getManagedPort(udpChannelPort0, bindAddress);
        manager.getManagedPort(udpChannelPort0, bindAddress);
        assertThrows(BindException.class, () -> manager.getManagedPort(udpChannelPort0, bindAddress));
    }

    @Test
    void shouldAllocateOnCyclingThroughRange() throws BindException
    {
        final InetSocketAddress bindAddress = new InetSocketAddress("localhost", 0);
        final WildcardPortManager manager = new WildcardPortManager(portRange, false);

        manager.getManagedPort(udpChannelPort0, bindAddress);
        manager.getManagedPort(udpChannelPort0, bindAddress);
        manager.getManagedPort(udpChannelPort0, bindAddress);
        manager.getManagedPort(udpChannelPort0, bindAddress);
        manager.freeManagedPort(new InetSocketAddress("localhost", 20000));
        assertThat(manager.getManagedPort(
            udpChannelPubControl, bindAddress), is(new InetSocketAddress("localhost", 20000)));
    }

    @Test
    void shouldAllocateSkippingInUsePort() throws BindException
    {
        final InetSocketAddress bindAddress = new InetSocketAddress("localhost", 0);
        final WildcardPortManager manager = new WildcardPortManager(portRange, false);

        manager.getManagedPort(udpChannelPort0, bindAddress);
        manager.getManagedPort(udpChannelPort0, new InetSocketAddress("localhost", 20001));
        assertThat(manager.getManagedPort(
            udpChannelPubControl, bindAddress), is(new InetSocketAddress("localhost", 20002)));
    }

    @Test
    void shouldAllocateSkippingInUseOnCycle() throws BindException
    {
        final InetSocketAddress bindAddress = new InetSocketAddress("localhost", 0);
        final WildcardPortManager manager = new WildcardPortManager(portRange, false);

        manager.getManagedPort(udpChannelPort0, new InetSocketAddress("localhost", 20000));
        manager.getManagedPort(udpChannelPort0, new InetSocketAddress("localhost", 20001));
        manager.getManagedPort(udpChannelPort0, new InetSocketAddress("localhost", 20002));
        manager.freeManagedPort(new InetSocketAddress("localhost", 20002));
        manager.getManagedPort(udpChannelPort0, new InetSocketAddress("localhost", 20003));
        assertThat(manager.getManagedPort(
            udpChannelPubControl, bindAddress), is(new InetSocketAddress("localhost", 20002)));
    }
}

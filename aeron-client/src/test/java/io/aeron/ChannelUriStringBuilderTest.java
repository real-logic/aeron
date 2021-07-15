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
package io.aeron;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ChannelUriStringBuilderTest
{
    @Test
    public void shouldValidateMedia()
    {
        assertThrows(IllegalStateException.class,
            () -> new ChannelUriStringBuilder().validate());
    }

    @Test
    public void shouldValidateEndpointOrControl()
    {
        assertThrows(IllegalStateException.class,
            () -> new ChannelUriStringBuilder().media("udp").validate());
    }

    @Test
    public void shouldValidateInitialPosition()
    {
        assertThrows(IllegalStateException.class,
            () -> new ChannelUriStringBuilder().media("udp").endpoint("address:port").termId(999).validate());
    }

    @Test
    public void shouldGenerateBasicIpcChannel()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("ipc");

        assertEquals("aeron:ipc", builder.build());
    }

    @Test
    public void shouldGenerateBasicUdpChannel()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:9999");

        assertEquals("aeron:udp?endpoint=localhost:9999", builder.build());
    }

    @Test
    public void shouldGenerateBasicUdpChannelSpy()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .prefix("aeron-spy")
            .media("udp")
            .endpoint("localhost:9999");

        assertEquals("aeron-spy:aeron:udp?endpoint=localhost:9999", builder.build());
    }

    @Test
    public void shouldGenerateComplexUdpChannel()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:9999")
            .ttl(9)
            .termLength(1024 * 128);

        assertEquals("aeron:udp?endpoint=localhost:9999|term-length=131072|ttl=9", builder.build());
    }

    @Test
    public void shouldGenerateReplayUdpChannel()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("address:9999")
            .termLength(1024 * 128)
            .initialTermId(777)
            .termId(999)
            .termOffset(64);

        assertEquals(
            "aeron:udp?endpoint=address:9999|term-length=131072|init-term-id=777|term-id=999|term-offset=64",
            builder.build());
    }

    @Test
    public void shouldGenerateChannelWithSocketParameters()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("address:9999")
            .socketSndbufLength(8192)
            .socketRcvbufLength(4096);

        assertEquals(
            "aeron:udp?endpoint=address:9999|so-sndbuf=8192|so-rcvbuf=4096",
            builder.build());
    }

    @Test
    public void shouldGenerateChannelWithReceiverWindow()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("address:9999")
            .receiverWindowLength(8192);

        assertEquals(
            "aeron:udp?endpoint=address:9999|rcv-wnd=8192",
            builder.build());
    }

    @Test
    void shouldBuildChannelBuilderUsingExistingStringWithAllTheFields()
    {
        final String uri = "aeron-spy:aeron:udp?endpoint=127.0.0.1:0|interface=127.0.0.1|control=127.0.0.2:0|" +
            "control-mode=manual|tags=2,4|alias=foo|cc=cubic|fc=min|reliable=false|ttl=16|mtu=8992|" +
            "term-length=1048576|init-term-id=5|term-offset=64|term-id=4353|session-id=2314234|gtag=3|linger=0|" +
            "sparse=true|eos=true|tether=false|group=false|ssc=true|so-sndbuf=8388608|so-rcvbuf=2097152|" +
            "rcv-wnd=1048576|media-rcv-ts-offset=reserved|rcv-ts-offset=0|channel-snd-ts-offset=8";

        final ChannelUri fromString = ChannelUri.parse(uri);
        final ChannelUri fromBuilder = ChannelUri.parse(new ChannelUriStringBuilder(uri).build());

        assertEquals(Collections.emptyMap(), fromString.diff(fromBuilder));
    }

    @Test
    void shouldBuildChannelBuilderUsingExistingStringWithTaggedSessionIdAndIpc()
    {
        final String uri = "aeron:ipc?session-id=tag:123456";

        final ChannelUri fromString = ChannelUri.parse(uri);
        final ChannelUri fromBuilder = ChannelUri.parse(new ChannelUriStringBuilder(uri).build());

        assertEquals(Collections.emptyMap(), fromString.diff(fromBuilder));
    }

    @Test
    void shouldRejectInvalidOffsets()
    {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ChannelUriStringBuilder().mediaReceiveTimestampOffset("breserved"));
        assertThrows(
            IllegalArgumentException.class,
            () -> new ChannelUriStringBuilder().receiveTimestampOffset("breserved"));
        assertThrows(
            IllegalArgumentException.class,
            () -> new ChannelUriStringBuilder().channelSendTimestampOffset("breserved"));
    }
}

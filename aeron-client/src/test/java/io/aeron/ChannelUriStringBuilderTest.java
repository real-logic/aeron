/*
 * Copyright 2014-2024 Real Logic Limited.
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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ChannelUriStringBuilderTest
{
    @Test
    void shouldValidateMedia()
    {
        assertThrows(IllegalStateException.class,
            () -> new ChannelUriStringBuilder().validate());
    }

    @Test
    void shouldValidateEndpointOrControl()
    {
        assertThrows(IllegalStateException.class,
            () -> new ChannelUriStringBuilder().media("udp").validate());
    }

    @Test
    void shouldValidateInitialPosition()
    {
        assertThrows(IllegalStateException.class,
            () -> new ChannelUriStringBuilder().media("udp").endpoint("address:port").termId(999).validate());
    }

    @Test
    void shouldGenerateBasicIpcChannel()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("ipc");

        assertEquals("aeron:ipc", builder.build());
    }

    @Test
    void shouldGenerateBasicUdpChannel()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:9999");

        assertEquals("aeron:udp?endpoint=localhost:9999", builder.build());
    }

    @Test
    void shouldGenerateBasicUdpChannelSpy()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .prefix("aeron-spy")
            .media("udp")
            .endpoint("localhost:9999");

        assertEquals("aeron-spy:aeron:udp?endpoint=localhost:9999", builder.build());
    }

    @Test
    void shouldGenerateComplexUdpChannel()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:9999")
            .ttl(9)
            .termLength(1024 * 128);

        assertEquals("aeron:udp?endpoint=localhost:9999|term-length=131072|ttl=9", builder.build());
    }

    @Test
    void shouldGenerateReplayUdpChannel()
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
    void shouldGenerateChannelWithSocketParameters()
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
    void shouldGenerateChannelWithReceiverWindow()
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
    void shouldGenerateChannelWithLingerTimeout()
    {
        final Long lingerNs = 987654321123456789L;
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("ipc")
            .linger(lingerNs);

        assertSame(lingerNs, builder.linger());
        assertEquals(
            "aeron:ipc?linger=987654321123456789",
            builder.build());
    }

    @Test
    void shouldGenerateChannelWithoutLingerTimeoutIfNullIsPassed()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("address:9999")
            .linger((Long)null);

        assertNull(builder.linger());
        assertEquals(
            "aeron:udp?endpoint=address:9999",
            builder.build());
    }

    @Test
    void shouldRejectNegativeLingerTimeout()
    {
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new ChannelUriStringBuilder().media("udp").endpoint("address:9999").linger(-1L));

        assertEquals("linger value cannot be negative: -1", exception.getMessage());
    }

    @Test
    void shouldCopyLingerTimeoutFromChannelUriHumanForm()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder();
        builder.linger(ChannelUri.parse("aeron:ipc?linger=7200s"));

        assertEquals(TimeUnit.HOURS.toNanos(2), builder.linger());
    }

    @Test
    void shouldCopyLingerTimeoutFromChannelUriNanoseconds()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder();
        builder.linger(ChannelUri.parse("aeron:udp?linger=19191919191919191"));

        assertEquals(19191919191919191L, builder.linger());
    }

    @Test
    void shouldCopyLingerTimeoutFromChannelUriNoValue()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder();
        builder.linger(ChannelUri.parse("aeron:udp?endpoint=localhost:8080"));

        assertNull(builder.linger());
    }

    @Test
    void shouldCopyLingerTimeoutFromChannelUriNegativeValue()
    {
        final ChannelUri channelUri = ChannelUri.parse("aeron:udp?linger=-1000");
        assertThrows(IllegalArgumentException.class, () -> new ChannelUriStringBuilder().linger(channelUri));
    }

    @Test
    void shouldBuildChannelBuilderUsingExistingStringWithAllTheFields()
    {
        final String uri = "aeron-spy:aeron:udp?endpoint=127.0.0.1:0|interface=127.0.0.1|control=127.0.0.2:0|" +
            "control-mode=manual|tags=2,4|alias=foo|cc=cubic|fc=min|reliable=false|ttl=16|mtu=8992|" +
            "term-length=1048576|init-term-id=5|term-offset=64|term-id=4353|session-id=2314234|gtag=3|" +
            "linger=100000055000001|sparse=true|eos=true|tether=false|group=false|ssc=true|so-sndbuf=8388608|" +
            "so-rcvbuf=2097152|rcv-wnd=1048576|media-rcv-ts-offset=reserved|channel-rcv-ts-offset=0|" +
            "channel-snd-ts-offset=8|response-endpoint=127.0.0.3:0|response-correlation-id=12345|nak-delay=100000|" +
            "untethered-window-limit-timeout=1000|untethered-resting-timeout=5000";

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
            () -> new ChannelUriStringBuilder().channelReceiveTimestampOffset("breserved"));
        assertThrows(
            IllegalArgumentException.class,
            () -> new ChannelUriStringBuilder().channelSendTimestampOffset("breserved"));
    }

    @Test
    void shouldRejectInvalidNakDelay()
    {
        assertThrows(IllegalArgumentException.class, () -> new ChannelUriStringBuilder().nakDelay("foo"));
    }

    @Test
    void shouldHandleNakDelayWithUnits()
    {
        assertEquals(1000L, new ChannelUriStringBuilder().nakDelay("1us").nakDelay());
        assertEquals(1L, new ChannelUriStringBuilder().nakDelay("1ns").nakDelay());
        assertEquals(1000000L, new ChannelUriStringBuilder().nakDelay("1ms").nakDelay());
    }

    @Test
    void shouldHandleUntetheredWindowLimitTimeoutWithUnits()
    {
        assertEquals(1000L, new ChannelUriStringBuilder()
            .untetheredWindowLimitTimeout("1us").untetheredWindowLimitTimeoutNs());
        assertEquals(1L, new ChannelUriStringBuilder()
            .untetheredWindowLimitTimeout("1ns").untetheredWindowLimitTimeoutNs());
        assertEquals(1000000L, new ChannelUriStringBuilder()
            .untetheredWindowLimitTimeout("1ms").untetheredWindowLimitTimeoutNs());
    }

    @Test
    void shouldHandleUntetheredRestingTimeoutWithUnits()
    {
        assertEquals(1000L, new ChannelUriStringBuilder()
            .untetheredRestingTimeout("1us").untetheredRestingTimeoutNs());
        assertEquals(1L, new ChannelUriStringBuilder()
            .untetheredRestingTimeout("1ns").untetheredRestingTimeoutNs());
        assertEquals(1000000L, new ChannelUriStringBuilder()
            .untetheredRestingTimeout("1ms").untetheredRestingTimeoutNs());
    }

    @Test
    void shouldHandleMaxRetransmits()
    {
        assertEquals(20, new ChannelUriStringBuilder()
            .maxResend(20)
            .maxResend());
        assertTrue(new ChannelUriStringBuilder().maxResend(20).build()
            .contains(CommonContext.MAX_RESEND_PARAM_NAME + "=20"));
        assertEquals(30, new ChannelUriStringBuilder()
            .maxResend(ChannelUri.parse(new ChannelUriStringBuilder().maxResend(30).build()))
            .maxResend());
    }
}

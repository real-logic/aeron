package io.aeron.driver;

import io.aeron.driver.media.UdpChannel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaggedMulticastFlowControlTest
{
    private static final int DEFAULT_GROUP_SIZE = 0;
    private static final long DEFAULT_RTAG = new TaggedMulticastFlowControl().getRtag();
    private static final long DEFAULT_TIMEOUT = new TaggedMulticastFlowControl().getReceiverTimeoutNs();

    private final TaggedMulticastFlowControl flowControl = new TaggedMulticastFlowControl();

    private static Stream<Arguments> validUris()
    {
        return Stream.of(
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged",
                DEFAULT_RTAG, DEFAULT_GROUP_SIZE, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,t:100ms",
                DEFAULT_RTAG, DEFAULT_GROUP_SIZE, 100_000_000),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,g:123",
                123, DEFAULT_GROUP_SIZE, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,g:3000000000",
                3_000_000_000L, DEFAULT_GROUP_SIZE, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,g:123,t:100ms",
                123, DEFAULT_GROUP_SIZE, 100_000_000),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,g:100/10",
                100, 10, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,g:/10",
                DEFAULT_RTAG, 10, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,g:100/10,t:100ms",
                100, 10, 100_000_000));
    }

    @ParameterizedTest
    @MethodSource("validUris")
    void shouldParseValidFlowControlConfiguration(
        final String uri,
        final long rtag,
        final int groupSize,
        final long timeout)
    {
        flowControl.initialize(UdpChannel.parse(uri), 0, 0);

        assertEquals(rtag, flowControl.getRtag());
        assertEquals(groupSize, flowControl.getRequiredGroupSize());
        assertEquals(timeout, flowControl.getReceiverTimeoutNs());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,g:",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,g:100/",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,g:/",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,t:",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,g:100,t:",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,t:100ms,g:100/",
    })
    void shouldFailWithInvalidUris(final String uri)
    {
        assertThrows(Exception.class, () -> flowControl.initialize(UdpChannel.parse(uri), 0, 0));
    }
}
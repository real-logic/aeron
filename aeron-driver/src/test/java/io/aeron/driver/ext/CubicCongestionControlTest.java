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
package io.aeron.driver.ext;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CubicCongestionControlTest
{
    public static final int CONTEXT_RECEIVER_WINDOW_LENGTH = 65536;
    public static final int CHANNEL_RECEIVER_WINDOW_LENGTH = 8192;
    public static final int MTU_LENGTH = 1024;

    private final CountersManager countersManager = mock(CountersManager.class);
    private final UnsafeBuffer tempBuffer = new UnsafeBuffer(new byte[8192]);
    private final UnsafeBuffer valuesBuffer = new UnsafeBuffer(new byte[8192]);
    private final MediaDriver.Context context = new MediaDriver.Context()
        .initialWindowLength(CONTEXT_RECEIVER_WINDOW_LENGTH)
        .tempBuffer(tempBuffer);
    private final UdpChannel channelWithWindow = UdpChannel.parse(
        "aeron:udp?endpoint=127.0.0.1:9999|rcv-wnd=" + CHANNEL_RECEIVER_WINDOW_LENGTH);
    private final UdpChannel channelWithoutWindow = UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:9999");
    private final int bigTermLength = 1_000_000;
    private final NanoClock nanoClock = new CachedNanoClock();

    @BeforeEach
    void setUp()
    {
        when(countersManager.valuesBuffer()).thenReturn(valuesBuffer);
    }

    @Test
    void shouldSetWindowLengthFromChannel()
    {
        final CubicCongestionControl cubicCongestionControl = new CubicCongestionControl(
            0, channelWithWindow, 0, 0, bigTermLength, MTU_LENGTH, null, null, nanoClock, context, countersManager);

        assertEquals(CHANNEL_RECEIVER_WINDOW_LENGTH / MTU_LENGTH, cubicCongestionControl.maxCongestionWindow());
    }

    @Test
    void shouldSetWindowLengthFromContext()
    {
        final CubicCongestionControl cubicCongestionControl = new CubicCongestionControl(
            0, channelWithoutWindow, 0, 0, bigTermLength, MTU_LENGTH, null, null, nanoClock, context, countersManager);

        assertEquals(CONTEXT_RECEIVER_WINDOW_LENGTH / MTU_LENGTH, cubicCongestionControl.maxCongestionWindow());
    }

    @Test
    void shouldSetWindowLengthFromTermLength()
    {
        final int smallTermLength = 8192;
        final CubicCongestionControl cubicCongestionControl = new CubicCongestionControl(
            0, channelWithWindow, 0, 0, smallTermLength, MTU_LENGTH, null, null, nanoClock, context, countersManager);

        assertEquals(smallTermLength / 2 / MTU_LENGTH, cubicCongestionControl.maxCongestionWindow());
    }
}

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
package io.aeron.driver;

import io.aeron.driver.media.UdpChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StaticWindowCongestionControlTest
{
    @Test
    void shouldSetWindowLengthFromChannel()
    {
        final UdpChannel channelWithWindow = UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:9999|rcv-wnd=8192");
        final MediaDriver.Context context = new MediaDriver.Context().initialWindowLength(16536);
        final int termLength = 1_000_000;

        final StaticWindowCongestionControl staticWindowCongestionControl = new StaticWindowCongestionControl(
            0, channelWithWindow, 0, 0, termLength, 0, null, null, null, context, null);

        assertEquals(8192, staticWindowCongestionControl.initialWindowLength());
    }

    @Test
    void shouldSetWindowLengthFromContext()
    {
        final UdpChannel channelWithoutWindow = UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:9999");
        final MediaDriver.Context context = new MediaDriver.Context().initialWindowLength(16536);
        final int termLength = 1_000_000;

        final StaticWindowCongestionControl staticWindowCongestionControl = new StaticWindowCongestionControl(
            0, channelWithoutWindow, 0, 0, termLength, 0, null, null, null, context, null);

        assertEquals(16536, staticWindowCongestionControl.initialWindowLength());
    }

    @Test
    void shouldSetWindowLengthFromTermLength()
    {
        final UdpChannel channelWithWindow = UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:9999|rcv-wnd=8192");
        final MediaDriver.Context context = new MediaDriver.Context().initialWindowLength(16536);
        final int termLength = 8192;

        final StaticWindowCongestionControl staticWindowCongestionControl = new StaticWindowCongestionControl(
            0, channelWithWindow, 0, 0, termLength, 0, null, null, null, context, null);

        assertEquals(termLength / 2, staticWindowCongestionControl.initialWindowLength());
    }
}
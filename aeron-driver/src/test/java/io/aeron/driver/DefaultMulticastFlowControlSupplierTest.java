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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultMulticastFlowControlSupplierTest
{
    private final DefaultMulticastFlowControlSupplier supplier = new DefaultMulticastFlowControlSupplier();

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=min",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=min,t:100ms",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=min,t:100ms,g:10",
    })
    void shouldReturnMinFlowControl(final String uri)
    {
        assertEquals(MinMulticastFlowControl.class, supplier.newInstance(UdpChannel.parse(uri), 0, 0).getClass());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=max",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=max,t:100ms",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=max,t:100ms,g:10",
    })
    void shouldReturnMaxFlowControl(final String uri)
    {
        assertEquals(MaxMulticastFlowControl.class, supplier.newInstance(UdpChannel.parse(uri), 0, 0).getClass());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,t:100ms",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,t:100ms,g:10",
    })
    void shouldReturnTaggedFlowControl(final String uri)
    {
        assertEquals(TaggedMulticastFlowControl.class, supplier.newInstance(UdpChannel.parse(uri), 0, 0).getClass());
    }


    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=minute",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=maximillian",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=taggedalong",
        "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=foobar",
    })
    void shouldRejectInvalidFlowControl(final String uri)
    {
        assertThrows(IllegalArgumentException.class, () -> supplier.newInstance(UdpChannel.parse(uri), 0, 0));
    }
}
/*
 *  Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ChannelUriStringBuilderTest
{
    @Test(expected = IllegalStateException.class)
    public void shouldValidateMedia()
    {
        new ChannelUriStringBuilder()
            .validate();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldValidateEndpointOrControl()
    {
        new ChannelUriStringBuilder()
            .media("udp")
            .validate();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldValidateInitialPosition()
    {
        new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("address:port")
            .termId(999)
            .validate();
    }

    @Test
    public void shouldGenerateBasicIpcChannel()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("ipc");

        assertThat(builder.build(), is("aeron:ipc"));
    }

    @Test
    public void shouldGenerateBasicUdpChannel()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:9999");

        assertThat(builder.build(), is("aeron:udp?endpoint=localhost:9999"));
    }

    @Test
    public void shouldGenerateBasicUdpChannelSpy()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .prefix("aeron-spy")
            .media("udp")
            .endpoint("localhost:9999");

        assertThat(builder.build(), is("aeron-spy:aeron:udp?endpoint=localhost:9999"));
    }

    @Test
    public void shouldGenerateComplexUdpChannel()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:9999")
            .ttl(9)
            .termLength(1024 * 128);

        assertThat(builder.build(), is("aeron:udp?endpoint=localhost:9999|ttl=9|term-length=131072"));
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

        assertThat(
            builder.build(),
            is("aeron:udp?endpoint=address:9999|term-length=131072|init-term-id=777|term-id=999|term-offset=64"));
    }
}
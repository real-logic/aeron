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
package io.aeron.driver;

import io.aeron.ChannelUri;

import io.aeron.CommonContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class PublicationParamsTest
{
    private final MediaDriver.Context ctx = new MediaDriver.Context();
    private final DriverConductor conductor = mock(DriverConductor.class);

    @Test
    void basicParse()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:1010");
        final PublicationParams params = PublicationParams.getPublicationParams(uri, ctx, conductor, false);

        assertFalse(params.hasMaxRetransmits);
    }

    @Test
    void hasMaxRetransmits()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:1010|" +
            CommonContext.MAX_RETRANSMITS_PARAM_NAME + "=1234");
        final PublicationParams params = PublicationParams.getPublicationParams(uri, ctx, conductor, false);

        assertTrue(params.hasMaxRetransmits);
        assertEquals(1234, params.maxRetransmits);
    }

    @Test
    void hasNegativeMaxRetransmits()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:1010|" +
            CommonContext.MAX_RETRANSMITS_PARAM_NAME + "=-1234");
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> PublicationParams.getPublicationParams(uri, ctx, conductor, false));

        assertTrue(exception.getMessage().contains("must be > 0"));
    }

    @Test
    void hasInvalidMaxRetransmits()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:1010|" +
            CommonContext.MAX_RETRANSMITS_PARAM_NAME + "=notanumber");
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> PublicationParams.getPublicationParams(uri, ctx, conductor, false));

        assertTrue(exception.getMessage().contains("must be a number"));
    }
}

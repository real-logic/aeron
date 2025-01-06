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

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DefaultNameResolverTest
{
    private final DefaultNameResolver nameResolver = new DefaultNameResolver();

    @Test
    void resolveReturnsNullForUnknownHost()
    {
        final String hostName = UUID.randomUUID().toString();
        final String uriParamName = "endpoint";
        final boolean isReResolution = false;

        assertNull(nameResolver.resolve(hostName, uriParamName, isReResolution));
    }

    @Test
    void resolveResolvesLocalhostAddress()
    {
        final String hostName = "localhost";
        final String uriParamName = "control";
        final boolean isReResolution = true;

        final InetAddress address = nameResolver.resolve(hostName, uriParamName, isReResolution);

        assertEquals(InetAddress.getLoopbackAddress(), address);
    }
}

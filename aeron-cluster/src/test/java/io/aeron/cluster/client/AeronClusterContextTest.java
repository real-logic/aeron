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
package io.aeron.cluster.client;

import io.aeron.Aeron;
import io.aeron.exceptions.ConfigurationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.mock;

class AeronClusterContextTest
{

    @ParameterizedTest
    @NullAndEmptySource
    void concludeThrowsConfigurationExceptionIfIngressChannelIsNotSet(final String ingressChannel)
    {
        final Aeron aeron = mock(Aeron.class);
        final AeronCluster.Context context = new AeronCluster.Context();
        context.aeron(aeron).ingressChannel(ingressChannel);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals("ERROR - ingressChannel must be specified", exception.getMessage());
    }

    @Test
    void concludeThrowsConfigurationExceptionIfIngressChannelIsSetToIpcAndIngressEndpointsSpecified()
    {
        final Aeron aeron = mock(Aeron.class);
        final AeronCluster.Context context = new AeronCluster.Context();
        context
            .aeron(aeron)
            .ingressChannel("aeron:ipc")
            .ingressEndpoints("0,localhost:1234");

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals(
            "ERROR - AeronCluster.Context ingressEndpoints must be null when using IPC ingress",
            exception.getMessage());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void concludeThrowsConfigurationExceptionIfEgressChannelIsNotSet(final String egressChannel)
    {
        final Aeron aeron = mock(Aeron.class);
        final AeronCluster.Context context = new AeronCluster.Context();
        context.aeron(aeron).ingressChannel("aeron:udp").egressChannel(egressChannel);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals("ERROR - egressChannel must be specified", exception.getMessage());
    }
}

/*
 * Copyright 2014-2022 Real Logic Limited.
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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class ClusteredServiceAgentTest
{
    @Test
    void shouldClaimAndWriteToBufferWhenFollower()
    {
        final Aeron aeron = mock(Aeron.class);
        final Publication publication = mock(Publication.class);

        final ClusteredServiceContainer.Context ctx = new ClusteredServiceContainer.Context()
            .aeron(aeron)
            .idleStrategySupplier(() -> YieldingIdleStrategy.INSTANCE);
        final ClusteredServiceAgent clusteredServiceAgent = new ClusteredServiceAgent(ctx);

        final BufferClaim bufferClaim = new BufferClaim();
        final int length = 64;
        final DirectBuffer msg = new UnsafeBuffer(new byte[length]);

        final long l = clusteredServiceAgent.tryClaim(0, publication, length, bufferClaim);
        assertEquals(ClientSession.MOCKED_OFFER, l);
        final MutableDirectBuffer buffer = bufferClaim.buffer();
        buffer.putBytes(bufferClaim.offset() + AeronCluster.SESSION_HEADER_LENGTH, msg, 0, length);
    }
}
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
package io.aeron.cluster;

import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.test.cluster.TestCluster.aCluster;

@ExtendWith(InterruptingTestCallback.class)
class MultiClusteredServicesTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    final AtomicLong serviceAMessageCount = new AtomicLong(0);
    final AtomicLong serviceBMessageCount = new AtomicLong(0);

    @BeforeEach
    void setUp()
    {
    }

    final class ServiceA extends TestNode.TestService
    {
        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            serviceAMessageCount.incrementAndGet();
        }
    }

    final class ServiceB extends TestNode.TestService
    {
        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            serviceBMessageCount.incrementAndGet();
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldSupportMultipleServicesPerNode()
    {
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withServiceSupplier(i -> new TestNode.TestService[]{ new ServiceA(), new ServiceB() })
            .start(3);
        systemTestWatcher.cluster(cluster);

        cluster.connectClient();
        cluster.sendMessages(3);

        Tests.awaitValue(serviceAMessageCount, 3);
        Tests.awaitValue(serviceBMessageCount, 3);
    }
}

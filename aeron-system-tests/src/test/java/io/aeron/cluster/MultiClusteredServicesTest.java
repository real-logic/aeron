/*
 * Copyright 2014-2021 Real Logic Limited.
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

import io.aeron.CommonContext;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MultiClusteredServicesTest
{
    final AtomicLong serviceAMessageCount = new AtomicLong(0);
    final AtomicLong serviceBMessageCount = new AtomicLong(0);

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
    @Timeout(20)
    public void shouldSupportMultipleServicesPerNode()
    {
        final List<TestCluster.NodeContext> nodeContexts = new ArrayList<>();
        final List<TestCluster.ServiceContext> serviceContexts = new ArrayList<>();
        final List<ClusteredMediaDriver> clusteredMediaDrivers = new ArrayList<>();
        final List<ClusteredServiceContainer> clusteredServiceContainers = new ArrayList<>();

        nodeContexts.add(TestCluster.nodeContext(0, true));
        nodeContexts.add(TestCluster.nodeContext(1, true));
        nodeContexts.add(TestCluster.nodeContext(2, true));

        serviceContexts.add(TestCluster.serviceContext(0, 0, nodeContexts.get(0), ServiceA::new));
        serviceContexts.add(TestCluster.serviceContext(0, 1, nodeContexts.get(0), ServiceB::new));
        serviceContexts.add(TestCluster.serviceContext(1, 0, nodeContexts.get(1), ServiceA::new));
        serviceContexts.add(TestCluster.serviceContext(1, 1, nodeContexts.get(1), ServiceB::new));
        serviceContexts.add(TestCluster.serviceContext(2, 0, nodeContexts.get(2), ServiceA::new));
        serviceContexts.add(TestCluster.serviceContext(2, 1, nodeContexts.get(2), ServiceB::new));

        nodeContexts.forEach((context) -> clusteredMediaDrivers.add(ClusteredMediaDriver.launch(
            context.mediaDriverCtx, context.archiveCtx, context.consensusModuleCtx)));

        serviceContexts.forEach(
            (context) ->
            {
                context.serviceContainerCtx.aeronDirectoryName(context.aeronCtx.aeronDirectoryName());
                clusteredServiceContainers.add(ClusteredServiceContainer.launch(context.serviceContainerCtx));
            });

        final String aeronDirName = CommonContext.getAeronDirectoryName();

        final MediaDriver clientMediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .aeronDirectoryName(aeronDirName));

        final AeronCluster client = AeronCluster.connect(new AeronCluster.Context()
            .aeronDirectoryName(aeronDirName)
            .ingressChannel("aeron:udp")
            .ingressEndpoints(TestCluster.ingressEndpoints(0, 3)));

        try
        {
            final DirectBuffer buffer = new UnsafeBuffer(new byte[100]);

            while (client.offer(buffer, 0, 100) < 0)
            {
                Tests.yield();
            }

            Tests.awaitValue(serviceAMessageCount, 3);
            Tests.awaitValue(serviceBMessageCount, 3);
        }
        finally
        {
            CloseHelper.closeAll(client, clientMediaDriver);

            clusteredMediaDrivers.forEach((clusteredMediaDriver) -> clusteredMediaDriver.consensusModule().close());
            CloseHelper.closeAll(clusteredServiceContainers);
            CloseHelper.closeAll(clusteredMediaDrivers);

            clientMediaDriver.context().deleteDirectory();
            clusteredMediaDrivers.forEach((driver) -> driver.mediaDriver().context().deleteDirectory());
        }
    }
}

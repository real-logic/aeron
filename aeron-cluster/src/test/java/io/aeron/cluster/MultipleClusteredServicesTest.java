package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MultipleClusteredServicesTest
{
    private final AtomicInteger serviceAMessageCount = new AtomicInteger(0);
    private final AtomicInteger serviceBMessageCount = new AtomicInteger(0);

    private final class ServiceA extends TestNode.TestService
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

    private final class ServiceB extends TestNode.TestService
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

    @Test(timeout = 10_000L)
    public void testMultiService()
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

        nodeContexts.forEach(context -> clusteredMediaDrivers.add(ClusteredMediaDriver.launch(
            context.mediaDriverContext, context.archiveContext, context.consensusModuleContext)));

        serviceContexts.forEach(context ->
        {
            final Aeron aeron = Aeron.connect(context.aeron);
            context.aeronArchiveContext.aeron(aeron).ownsAeronClient(false);
            clusteredServiceContainers.add(
                ClusteredServiceContainer.launch(context.serviceContainerContext.aeron(aeron).ownsAeronClient(true)));
        });

        final String aeronDirName = CommonContext.getAeronDirectoryName();

        final MediaDriver clientMediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .aeronDirectoryName(aeronDirName));

        final AeronCluster client = AeronCluster.connect(new AeronCluster.Context()
            .aeronDirectoryName(aeronDirName)
            .ingressChannel("aeron:udp")
            .clusterMemberEndpoints(TestCluster.clientMemberEndpoints(3)));

        try
        {
            while (client.offer(new ExpandableArrayBuffer(100), 0, 100) < 0)
            {
                Thread.yield();
            }

            // Comment out the while loop to see more failures.
            while (serviceAMessageCount.get() < 3)
            {
                Thread.yield();
            }

            while (serviceBMessageCount.get() < 3)
            {
                Thread.yield();
            }
        }
        finally
        {
            CloseHelper.closeAll(client, clientMediaDriver);
            clusteredServiceContainers.forEach(CloseHelper::close);
            clusteredMediaDrivers.forEach(CloseHelper::close);
        }
    }
}

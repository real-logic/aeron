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
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.test.Tests;
import io.aeron.test.cluster.StubClusteredService;
import io.aeron.test.cluster.TestCluster;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiModuleSharedDriverTest
{
    @Test
    @Timeout(20)
    public void shouldSupportTwoSingleNodeClusters()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .errorHandler(Tests::onError)
            .dirDeleteOnStart(true);

        final Archive.Context archiveCtx = new Archive.Context()
            .threadingMode(ArchiveThreadingMode.SHARED)
            .archiveDir(new File(SystemUtil.tmpDirName(), "archive"))
            .errorHandler(Tests::onError)
            .recordingEventsEnabled(false)
            .deleteArchiveOnStart(true);

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx, archiveCtx))
        {
            final ConsensusModule.Context moduleCtx0 = new ConsensusModule.Context()
                .clusterId(0)
                .errorHandler(Tests::onError)
                .deleteDirOnStart(true)
                .clusterDir(new File(SystemUtil.tmpDirName(), "cluster-0-0"))
                .logChannel("aeron:ipc?term-length=64k")
                .logStreamId(100)
                .serviceStreamId(104)
                .consensusModuleStreamId(105)
                .ingressChannel("aeron:udp?endpoint=localhost:9010");

            final ClusteredServiceContainer.Context containerCtx0 = new ClusteredServiceContainer.Context()
                .clusterId(moduleCtx0.clusterId())
                .errorHandler(Tests::onError)
                .clusteredService(new EchoService())
                .clusterDir(moduleCtx0.clusterDir())
                .serviceStreamId(moduleCtx0.serviceStreamId())
                .consensusModuleStreamId(moduleCtx0.consensusModuleStreamId());

            final ConsensusModule.Context moduleCtx1 = new ConsensusModule.Context()
                .clusterId(1)
                .errorHandler(Tests::onError)
                .deleteDirOnStart(true)
                .clusterDir(new File(SystemUtil.tmpDirName(), "cluster-0-1"))
                .logChannel("aeron:ipc?term-length=64k")
                .logStreamId(200)
                .serviceStreamId(204)
                .consensusModuleStreamId(205)
                .ingressChannel("aeron:udp?endpoint=localhost:9011");

            final ClusteredServiceContainer.Context containerCtx1 = new ClusteredServiceContainer.Context()
                .errorHandler(Tests::onError)
                .clusteredService(new EchoService())
                .clusterDir(moduleCtx1.clusterDir())
                .serviceStreamId(moduleCtx1.serviceStreamId())
                .consensusModuleStreamId(moduleCtx1.consensusModuleStreamId())
                .clusterId(moduleCtx1.clusterId());

            ConsensusModule consensusModule0 = null;
            ClusteredServiceContainer container0 = null;
            ConsensusModule consensusModule1 = null;
            ClusteredServiceContainer container1 = null;
            AeronCluster client0 = null;
            AeronCluster client1 = null;

            try
            {
                consensusModule0 = ConsensusModule.launch(moduleCtx0);
                consensusModule1 = ConsensusModule.launch(moduleCtx1);

                container0 = ClusteredServiceContainer.launch(containerCtx0);
                container1 = ClusteredServiceContainer.launch(containerCtx1);

                final MutableReference<String> egress = new MutableReference<>();
                final EgressListener egressListener = (clusterSessionId, timestamp, buffer, offset, length, header) ->
                    egress.set(buffer.getStringWithoutLengthAscii(offset, length));

                client0 = AeronCluster.connect(new AeronCluster.Context()
                    .egressListener(egressListener)
                    .ingressChannel(moduleCtx0.ingressChannel())
                    .egressChannel("aeron:udp?endpoint=localhost:9020"));

                client1 = AeronCluster.connect(new AeronCluster.Context()
                    .egressListener(egressListener)
                    .ingressChannel(moduleCtx1.ingressChannel())
                    .egressChannel("aeron:udp?endpoint=localhost:9021"));

                echoMessage(client0, "Message 0", egress);
                echoMessage(client1, "Message 1", egress);
            }
            finally
            {
                CloseHelper.closeAll(client0, client1, consensusModule0, consensusModule1, container0, container1);
                moduleCtx0.deleteDirectory();
                moduleCtx1.deleteDirectory();
            }
        }
        finally
        {
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    @Test
    @Timeout(30)
    public void shouldSupportTwoMultiNodeClusters()
    {
        try (MultiClusterNode node0 = new MultiClusterNode(0);
            MultiClusterNode node1 = new MultiClusterNode(1))
        {
            final MutableReference<String> egress = new MutableReference<>();
            final EgressListener egressListener = (clusterSessionId, timestamp, buffer, offset, length, header) ->
                egress.set(buffer.getStringWithoutLengthAscii(offset, length));

            try (
                AeronCluster client0 = AeronCluster.connect(new AeronCluster.Context()
                    .aeronDirectoryName(node0.archivingMediaDriver.mediaDriver().aeronDirectoryName())
                    .egressListener(egressListener)
                    .ingressChannel("aeron:udp?term-length=64k")
                    .ingressEndpoints(TestCluster.ingressEndpoints(0, 2))
                    .egressChannel("aeron:udp?endpoint=localhost:9020"));
                AeronCluster client1 = AeronCluster.connect(new AeronCluster.Context()
                    .aeronDirectoryName(node1.archivingMediaDriver.mediaDriver().aeronDirectoryName())
                    .egressListener(egressListener)
                    .ingressChannel("aeron:udp?term-length=64k")
                    .ingressEndpoints(TestCluster.ingressEndpoints(1, 2))
                    .egressChannel("aeron:udp?endpoint=localhost:9120")))
            {
                echoMessage(client0, "Message 0", egress);
                echoMessage(client1, "Message 1", egress);
            }
        }
    }

    private void echoMessage(final AeronCluster client, final String message, final MutableReference<String> egress)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int length = buffer.putStringWithoutLengthAscii(0, message);

        while (client.offer(buffer, 0, length) < 0)
        {
            Tests.yield();
        }

        egress.set(null);
        while (null == egress.get())
        {
            Tests.yield();
            client.pollEgress();
        }

        assertEquals(message, egress.get());
    }

    static class EchoService extends StubClusteredService
    {
        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            idleStrategy.reset();
            while (session.offer(buffer, offset, length) < 0)
            {
                idleStrategy.idle();
            }
        }
    }

    static class MultiClusterNode implements AutoCloseable
    {
        final int nodeId;
        final ArchivingMediaDriver archivingMediaDriver;

        final ConsensusModule consensusModule0;
        final ClusteredServiceContainer container0;
        AeronCluster client0;

        final ConsensusModule consensusModule1;
        final ClusteredServiceContainer container1;
        AeronCluster client1;

        MultiClusterNode(final int nodeId)
        {
            this.nodeId = nodeId;

            final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(CommonContext.getAeronDirectoryName() + "-" + nodeId)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Tests::onError)
                .dirDeleteOnStart(true);

            final Archive.Context archiveCtx = new Archive.Context()
                .threadingMode(ArchiveThreadingMode.SHARED)
                .archiveDir(new File(SystemUtil.tmpDirName(), "archive-" + nodeId))
                .controlChannel("aeron:udp?endpoint=localhost:801" + nodeId)
                .errorHandler(Tests::onError)
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(true);

            archivingMediaDriver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            consensusModule0 = consensusModule(0, driverCtx.aeronDirectoryName());
            container0 = container(consensusModule0.context());
            consensusModule1 = consensusModule(1, driverCtx.aeronDirectoryName());
            container1 = container(consensusModule1.context());
        }

        public void close()
        {
            CloseHelper.closeAll(
                client0,
                consensusModule0,
                container0,
                client1,
                consensusModule1,
                container1,
                archivingMediaDriver);

            consensusModule0.context().deleteDirectory();
            consensusModule1.context().deleteDirectory();
            archivingMediaDriver.archive().context().deleteDirectory();
            archivingMediaDriver.mediaDriver().context().deleteDirectory();
        }

        ConsensusModule consensusModule(final int clusterId, final String aeronDirectoryName)
        {
            final int nodeOffset = (clusterId * 100) + (nodeId * 10);
            final ConsensusModule.Context ctx = new ConsensusModule.Context()
                .clusterMemberId(nodeId)
                .clusterId(clusterId)
                .errorHandler(Tests::onError)
                .deleteDirOnStart(true)
                .aeronDirectoryName(aeronDirectoryName)
                .clusterDir(new File(SystemUtil.tmpDirName(), "cluster-" + nodeId + "-" + clusterId))
                .clusterMembers(TestCluster.clusterMembers(clusterId, 2))
                .logChannel("aeron:udp?term-length=64k")
                .serviceStreamId(104 + nodeOffset)
                .consensusModuleStreamId(105 + nodeOffset)
                .ingressChannel("aeron:udp?term-length=64k");

            return ConsensusModule.launch(ctx);
        }

        ClusteredServiceContainer container(final ConsensusModule.Context moduleCtx)
        {
            final ClusteredServiceContainer.Context ctx = new ClusteredServiceContainer.Context()
                .clusterId(moduleCtx.clusterId())
                .errorHandler(Tests::onError)
                .clusteredService(new EchoService())
                .aeronDirectoryName(moduleCtx.aeronDirectoryName())
                .clusterDir(moduleCtx.clusterDir())
                .serviceStreamId(moduleCtx.serviceStreamId())
                .consensusModuleStreamId(moduleCtx.consensusModuleStreamId());

            return ClusteredServiceContainer.launch(ctx);
        }
    }
}

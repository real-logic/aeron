/*
 * Copyright 2014-2020 Real Logic Limited.
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

import io.aeron.archive.*;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.*;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.test.Tests;
import org.agrona.*;
import org.agrona.collections.MutableReference;
import org.junit.jupiter.api.*;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiModuleSharedDriverTest
{
    @Test
    @Timeout(20)
    @SuppressWarnings("MethodLength")
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
                .errorHandler(Tests::onError)
                .deleteDirOnStart(true)
                .clusterDir(new File(SystemUtil.tmpDirName(), "cluster-zero"))
                .logChannel("aeron:ipc?term-length=64k")
                .logStreamId(100)
                .serviceControlChannel("aeron:ipc?term-length=64k")
                .serviceStreamId(104)
                .consensusModuleStreamId(105)
                .ingressChannel("aeron:udp?endpoint=localhost:9010")
                .clusterId(0);

            final ClusteredServiceContainer.Context containerCtx0 = new ClusteredServiceContainer.Context()
                .errorHandler(Tests::onError)
                .clusteredService(new EchoService())
                .clusterDir(moduleCtx0.clusterDir())
                .serviceControlChannel(moduleCtx0.serviceControlChannel())
                .serviceStreamId(moduleCtx0.serviceStreamId())
                .consensusModuleStreamId(moduleCtx0.consensusModuleStreamId())
                .clusterId(moduleCtx0.clusterId());

            final ConsensusModule.Context moduleCtx1 = new ConsensusModule.Context()
                .errorHandler(Tests::onError)
                .deleteDirOnStart(true)
                .clusterDir(new File(SystemUtil.tmpDirName(), "cluster-one"))
                .logChannel("aeron:ipc?term-length=64k")
                .logStreamId(200)
                .serviceControlChannel("aeron:ipc?term-length=64k")
                .serviceStreamId(204)
                .consensusModuleStreamId(205)
                .ingressChannel("aeron:udp?endpoint=localhost:9011")
                .clusterId(1);

            final ClusteredServiceContainer.Context containerCtx1 = new ClusteredServiceContainer.Context()
                .errorHandler(Tests::onError)
                .clusteredService(new EchoService())
                .clusterDir(moduleCtx1.clusterDir())
                .serviceControlChannel(moduleCtx1.serviceControlChannel())
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
            }
        }
        finally
        {
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    private void echoMessage(final AeronCluster client, final String message, final MutableReference<String> egress)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int length = buffer.putStringWithoutLengthAscii(0, message);

        while (client.offer(buffer, 0, length) < 0)
        {
            Thread.yield();
            Tests.checkInterruptStatus();
        }

        egress.set(null);
        while (null == egress.get())
        {
            Thread.yield();
            Tests.checkInterruptStatus();
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
}

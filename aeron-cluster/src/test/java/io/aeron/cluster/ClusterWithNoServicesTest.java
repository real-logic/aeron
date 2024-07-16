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
package io.aeron.cluster;

import io.aeron.Image;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.TestContexts;
import io.aeron.test.cluster.ClusterTests;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class ClusterWithNoServicesTest
{
    private final ConsensusModuleExtension consensusModuleExtensionSpy = spy(new TestConsensusModuleExtension());
    private ClusteredMediaDriver clusteredMediaDriver;
    private AeronCluster aeronCluster;

    @AfterEach
    void after()
    {
        final ConsensusModule consensusModule = null == clusteredMediaDriver ?
            null : clusteredMediaDriver.consensusModule();

        CloseHelper.closeAll(aeronCluster, consensusModule, clusteredMediaDriver);

        if (null != clusteredMediaDriver)
        {
            clusteredMediaDriver.consensusModule().context().deleteDirectory();
            clusteredMediaDriver.archive().context().deleteDirectory();
            clusteredMediaDriver.mediaDriver().context().deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldConnectAndSendKeepAliveWithExtensionLoaded()
    {
        clusteredMediaDriver = launchCluster();
        aeronCluster = connectClient();

        assertTrue(aeronCluster.sendKeepAlive());

        final InOrder inOrder = inOrder(consensusModuleExtensionSpy);
        inOrder.verify(consensusModuleExtensionSpy).onStart(any(ConsensusModuleControl.class), isNull());
        inOrder.verify(consensusModuleExtensionSpy).onElectionComplete(any(ConsensusControlState.class));
        inOrder.verify(consensusModuleExtensionSpy, atLeastOnce()).doWork(anyLong());

        verify(consensusModuleExtensionSpy).onSessionOpened(anyLong());

        ClusterTests.failOnClusterError();
    }

    private ClusteredMediaDriver launchCluster()
    {
        return ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .errorHandler(ClusterTests.errorHandler(0))
                .dirDeleteOnStart(true),
            TestContexts.localhostArchive()
                .catalogCapacity(ClusterTestConstants.CATALOG_CAPACITY)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(true),
            new ConsensusModule.Context()
                .errorHandler(ClusterTests.errorHandler(0))
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .clusterMembers(ClusterTestConstants.CLUSTER_MEMBERS)
                .ingressChannel("aeron:udp")
                .serviceCount(0)
                .consensusModuleExtension(consensusModuleExtensionSpy)
                .logChannel("aeron:ipc")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .deleteDirOnStart(true));
    }

    private static AeronCluster connectClient()
    {
        return AeronCluster.connect(
            new AeronCluster.Context()
                .ingressChannel("aeron:udp")
                .ingressEndpoints(ClusterTestConstants.INGRESS_ENDPOINTS)
                .egressChannel("aeron:udp?endpoint=localhost:0"));
    }

    static final class TestConsensusModuleExtension implements ConsensusModuleExtension
    {
        public int supportedSchemaId()
        {
            return 0;
        }

        public void onStart(final ConsensusModuleControl consensusModuleControl, final Image snapshotImage)
        {
        }

        public int doWork(final long nowNs)
        {
            return 0;
        }

        public void onElectionComplete(final ConsensusControlState consensusControlState)
        {
        }

        public void onNewLeadershipTerm(final ConsensusControlState consensusControlState)
        {
        }

        public ControlledFragmentHandler.Action onIngressMessage(
            final int actingBlockLength,
            final int templateId,
            final int schemaId,
            final int actingVersion,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            return null;
        }

        public ControlledFragmentHandler.Action onLogMessage(
            final int actingBlockLength,
            final int templateId,
            final int schemaId,
            final int actingVersion,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            return null;
        }

        public void close()
        {
        }

        public void onSessionOpened(final long clusterSessionId)
        {
        }

        public void onSessionClosed(final long clusterSessionId)
        {
        }
    }
}

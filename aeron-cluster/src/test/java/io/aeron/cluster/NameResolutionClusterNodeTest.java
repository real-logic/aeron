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
package io.aeron.cluster;

import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.exceptions.InvalidChannelException;
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.StubClusteredService;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import static io.aeron.cluster.ClusterTestConstants.CLUSTER_MEMBERS;
import static io.aeron.cluster.ClusterTestConstants.INGRESS_ENDPOINTS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(InterruptingTestCallback.class)
class NameResolutionClusterNodeTest
{
    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private AeronCluster aeronCluster;

    private final ErrorHandler mockErrorHandler = mock(ErrorHandler.class);

    @BeforeEach
    void before()
    {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .errorHandler(mockErrorHandler)
                .dirDeleteOnStart(true),
            TestContexts.localhostArchive()
                .catalogCapacity(ClusterTestConstants.CATALOG_CAPACITY)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(true),
            new ConsensusModule.Context()
                .errorHandler(ClusterTests.errorHandler(0))
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .logChannel("aeron:ipc")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .ingressChannel("aeron:udp")
                .clusterMembers(CLUSTER_MEMBERS)
                .deleteDirOnStart(true));
    }

    @AfterEach
    void after()
    {
        final ConsensusModule consensusModule = null == clusteredMediaDriver ?
            null : clusteredMediaDriver.consensusModule();

        CloseHelper.closeAll(aeronCluster, consensusModule, container, clusteredMediaDriver);

        if (null != clusteredMediaDriver)
        {
            clusteredMediaDriver.consensusModule().context().deleteDirectory();
            clusteredMediaDriver.archive().context().deleteDirectory();
            clusteredMediaDriver.mediaDriver().context().deleteDirectory();
            container.context().deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldConnectAndSendKeepAliveWithBadName()
    {
        container = launchEchoService();
        aeronCluster = connectToCluster();

        assertTrue(aeronCluster.sendKeepAlive());

        final ArgumentCaptor<Throwable> argumentCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(mockErrorHandler).onError(argumentCaptor.capture());

        assertEquals(InvalidChannelException.class, argumentCaptor.getValue().getClass());
        assertThat(argumentCaptor.getValue().getMessage(), containsString("badname"));

        ClusterTests.failOnClusterError();
    }

    private ClusteredServiceContainer launchEchoService()
    {
        final ClusteredService clusteredService = new StubClusteredService()
        {
            public void onSessionMessage(
                final ClientSession session,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                echoMessage(session, buffer, offset, length);
            }
        };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(clusteredService)
                .errorHandler(Tests::onError));
    }

    private AeronCluster connectToCluster()
    {
        final ErrorHandler errorHandler =
            (t) ->
            {
                System.err.println("** MY HANDLER **");
                t.printStackTrace();
            };

        return AeronCluster.connect(
            new AeronCluster.Context()
                .errorHandler(errorHandler)
                .ingressChannel("aeron:udp")
                .ingressEndpoints(INGRESS_ENDPOINTS + ",1=badname:9011")
                .egressChannel("aeron:udp?endpoint=localhost:0"));
    }
}

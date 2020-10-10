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

import io.aeron.*;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.test.DataCollector;
import io.aeron.test.Tests;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ConsensusModule.Configuration.SNAPSHOT_CHANNEL_DEFAULT;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

public class TestCluster implements AutoCloseable
{
    private static final int SEGMENT_FILE_LENGTH = 16 * 1024 * 1024;
    private static final long MAX_CATALOG_ENTRIES = 128;
    private static final String LOG_CHANNEL = "aeron:udp?term-length=512k";
    private static final String ARCHIVE_CONTROL_REQUEST_CHANNEL =
        "aeron:udp?term-length=64k|endpoint=localhost:8010";
    private static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL =
        "aeron:udp?term-length=64k|endpoint=localhost:8020";
    private static final String ARCHIVE_LOCAL_CONTROL_CHANNEL = "aeron:ipc?term-length=64k";
    private static final String EGRESS_CHANNEL = "aeron:udp?term-length=128k|endpoint=localhost:9020";
    private static final String INGRESS_CHANNEL = "aeron:udp?term-length=128k";

    private final DataCollector dataCollector = new DataCollector();
    private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
    private final MutableInteger responseCount = new MutableInteger();
    private final MutableInteger newLeaderEvent = new MutableInteger();
    private final EgressListener egressMessageListener = new EgressListener()
    {
        public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            responseCount.increment();
        }

        public void onSessionEvent(
            final long correlationId,
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final EventCode code,
            final String detail)
        {
            if (EventCode.ERROR == code)
            {
                throw new ClusterException(detail);
            }
            else if (EventCode.CLOSED == code && shouldPrintClientCloseReason)
            {
                System.out.println("session closed due to " + detail);
            }
        }

        public void onNewLeader(
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final String ingressEndpoints)
        {
            newLeaderEvent.increment();
        }
    };

    private final TestNode[] nodes;
    private final String staticClusterMembers;
    private final String staticClusterMemberEndpoints;
    private final String[] clusterMembersEndpoints;
    private final String clusterConsensusEndpoints;
    private final int staticMemberCount;
    private final int dynamicMemberCount;
    private final int appointedLeaderId;
    private final int backupNodeIndex;
    private boolean shouldPrintClientCloseReason = true;

    private MediaDriver clientMediaDriver;
    private AeronCluster client;
    private TestBackupNode backupNode;

    TestCluster(final int staticMemberCount, final int dynamicMemberCount, final int appointedLeaderId)
    {
        final int memberCount = staticMemberCount + dynamicMemberCount;
        if ((memberCount + 1) >= 10)
        {
            throw new IllegalArgumentException("max members exceeded: max=9 count=" + memberCount);
        }

        this.nodes = new TestNode[memberCount + 1];
        this.backupNodeIndex = memberCount;
        this.staticClusterMembers = clusterMembers(0, staticMemberCount);
        this.staticClusterMemberEndpoints = ingressEndpoints(0, staticMemberCount);
        this.clusterMembersEndpoints = clusterMembersEndpoints(0, memberCount);
        this.clusterConsensusEndpoints = clusterConsensusEndpoints(0, staticMemberCount);
        this.staticMemberCount = staticMemberCount;
        this.dynamicMemberCount = dynamicMemberCount;
        this.appointedLeaderId = appointedLeaderId;
    }

    static void awaitElectionClosed(final TestNode follower)
    {
        while (follower.electionState() != ElectionState.CLOSED)
        {
            Tests.sleep(10);
        }
    }

    static ClusterTool.ClusterMembership awaitMembershipSize(final TestNode leader, final int size)
    {
        while (true)
        {
            final ClusterTool.ClusterMembership clusterMembership = leader.clusterMembership();
            if (clusterMembership.activeMembers.size() == size)
            {
                return clusterMembership;
            }
            Tests.sleep(100);
        }
    }

    public void close()
    {
        final boolean isInterrupted = Thread.interrupted();
        try
        {
            CloseHelper.closeAll(
                client,
                clientMediaDriver,
                null != clientMediaDriver ? () -> clientMediaDriver.context().deleteDirectory() : null,
                () -> CloseHelper.closeAll(Stream.of(nodes).map(TestCluster::closeAndDeleteNode).collect(toList())),
                null != backupNode ? () -> backupNode.closeAndDelete() : null);
        }
        finally
        {
            if (isInterrupted)
            {
                Thread.currentThread().interrupt();
            }
        }

        ClusterTests.failOnClusterError();
    }

    static AutoCloseable closeAndDeleteNode(final TestNode node)
    {
        if (node == null)
        {
            return null;
        }

        return node::closeAndDelete;
    }

    static TestCluster startThreeNodeStaticCluster(final int appointedLeaderId)
    {
        final TestCluster testCluster = new TestCluster(3, 0, appointedLeaderId);
        for (int i = 0; i < 3; i++)
        {
            testCluster.startStaticNode(i, true);
        }

        return testCluster;
    }

    static TestCluster startSingleNodeStaticCluster()
    {
        final TestCluster testCluster = new TestCluster(1, 0, 0);
        testCluster.startStaticNode(0, true);

        return testCluster;
    }

    static TestCluster startCluster(final int staticMemberCount, final int dynamicMemberCount)
    {
        final TestCluster testCluster = new TestCluster(staticMemberCount, dynamicMemberCount, NULL_VALUE);
        for (int i = 0; i < staticMemberCount; i++)
        {
            testCluster.startStaticNode(i, true);
        }

        return testCluster;
    }

    TestNode startStaticNode(final int index, final boolean cleanStart)
    {
        return startStaticNode(index, cleanStart, TestNode.TestService::new);
    }

    TestNode startStaticNode(
        final int index, final boolean cleanStart, final Supplier<? extends TestNode.TestService> serviceSupplier)
    {
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final TestNode.Context context = new TestNode.Context(serviceSupplier.get().index(index));

        context.aeronArchiveContext
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, index))
            .controlRequestStreamId(100)
            .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, index))
            .controlResponseStreamId(110 + index)
            .aeronDirectoryName(baseDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .errorHandler(ClusterTests.errorHandler(index))
            .dirDeleteOnShutdown(false)
            .dirDeleteOnStart(true);

        context.archiveContext
            .maxCatalogEntries(MAX_CATALOG_ENTRIES)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .controlStreamId(context.aeronArchiveContext.controlRequestStreamId())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .recordingEventsEnabled(false)
            .localControlStreamId(context.aeronArchiveContext.controlRequestStreamId())
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(cleanStart);

        context.consensusModuleContext
            .errorHandler(ClusterTests.errorHandler(index))
            .clusterMemberId(index)
            .clusterMembers(staticClusterMembers)
            .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(5))
            .appointedLeaderId(appointedLeaderId)
            .clusterDir(new File(baseDirName, "consensus-module"))
            .ingressChannel(INGRESS_CHANNEL)
            .logChannel(LOG_CHANNEL)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .deleteDirOnStart(cleanStart);

        context.serviceContainerContext
            .aeronDirectoryName(aeronDirName)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .clusterDir(new File(baseDirName, "service"))
            .clusteredService(context.service)
            .errorHandler(ClusterTests.errorHandler(index));

        nodes[index] = new TestNode(context, dataCollector);

        return nodes[index];
    }

    TestNode startDynamicNode(final int index, final boolean cleanStart)
    {
        return startDynamicNode(index, cleanStart, TestNode.TestService::new);
    }

    TestNode startDynamicNode(
        final int index, final boolean cleanStart, final Supplier<? extends TestNode.TestService> serviceSupplier)
    {
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final TestNode.Context context = new TestNode.Context(serviceSupplier.get().index(index));

        context.aeronArchiveContext
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, index))
            .controlRequestStreamId(100)
            .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, index))
            .controlResponseStreamId(110 + index)
            .aeronDirectoryName(baseDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .errorHandler(ClusterTests.errorHandler(index))
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false);

        context.archiveContext
            .maxCatalogEntries(MAX_CATALOG_ENTRIES)
            .segmentFileLength(SEGMENT_FILE_LENGTH)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .controlStreamId(context.aeronArchiveContext.controlRequestStreamId())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .localControlStreamId(context.aeronArchiveContext.controlRequestStreamId())
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(cleanStart);

        context.consensusModuleContext
            .errorHandler(ClusterTests.errorHandler(index))
            .clusterMemberId(NULL_VALUE)
            .clusterMembers("")
            .clusterConsensusEndpoints(clusterConsensusEndpoints)
            .memberEndpoints(clusterMembersEndpoints[index])
            .clusterDir(new File(baseDirName, "consensus-module"))
            .ingressChannel(INGRESS_CHANNEL)
            .logChannel(LOG_CHANNEL)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .deleteDirOnStart(cleanStart);

        context.serviceContainerContext
            .aeronDirectoryName(aeronDirName)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .clusterDir(new File(baseDirName, "service"))
            .clusteredService(context.service)
            .errorHandler(ClusterTests.errorHandler(index));

        nodes[index] = new TestNode(context, dataCollector);

        return nodes[index];
    }

    TestNode startStaticNodeFromDynamicNode(final int index)
    {
        return startStaticNodeFromDynamicNode(index, TestNode.TestService::new);
    }

    TestNode startStaticNodeFromDynamicNode(
        final int index, final Supplier<? extends TestNode.TestService> serviceSupplier)
    {
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final TestNode.Context context = new TestNode.Context(serviceSupplier.get().index(index));

        context.aeronArchiveContext
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, index))
            .controlRequestStreamId(100)
            .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, index))
            .controlResponseStreamId(110 + index)
            .aeronDirectoryName(baseDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .errorHandler(ClusterTests.errorHandler(index))
            .dirDeleteOnShutdown(false)
            .dirDeleteOnStart(true);

        context.archiveContext
            .maxCatalogEntries(MAX_CATALOG_ENTRIES)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .controlStreamId(context.aeronArchiveContext.controlRequestStreamId())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .recordingEventsEnabled(false)
            .localControlStreamId(context.aeronArchiveContext.controlRequestStreamId())
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(false);

        context.consensusModuleContext
            .errorHandler(ClusterTests.errorHandler(index))
            .clusterMemberId(index)
            .clusterMembers(clusterMembers(0, staticMemberCount + 1))
            .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(5))
            .appointedLeaderId(appointedLeaderId)
            .clusterDir(new File(baseDirName, "consensus-module"))
            .ingressChannel(INGRESS_CHANNEL)
            .logChannel(LOG_CHANNEL)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .deleteDirOnStart(false);

        context.serviceContainerContext
            .aeronDirectoryName(aeronDirName)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .clusterDir(new File(baseDirName, "service"))
            .clusteredService(context.service)
            .errorHandler(ClusterTests.errorHandler(index));

        nodes[index] = new TestNode(context, dataCollector);

        return nodes[index];
    }

    TestBackupNode startClusterBackupNode(final boolean cleanStart)
    {
        final int index = staticMemberCount + dynamicMemberCount;
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final TestBackupNode.Context context = new TestBackupNode.Context();

        context.aeronArchiveContext
            .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, index))
            .controlRequestStreamId(100)
            .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, index))
            .controlResponseStreamId(110 + index)
            .aeronDirectoryName(baseDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .errorHandler(ClusterTests.errorHandler(index))
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false);

        context.archiveContext
            .maxCatalogEntries(MAX_CATALOG_ENTRIES)
            .segmentFileLength(SEGMENT_FILE_LENGTH)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .controlStreamId(context.aeronArchiveContext.controlRequestStreamId())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .localControlStreamId(context.aeronArchiveContext.controlRequestStreamId())
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(cleanStart);

        final ChannelUri consensusChannelUri = ChannelUri.parse(context.clusterBackupContext.consensusChannel());
        final String backupStatusEndpoint = clusterBackupStatusEndpoint(0, staticMemberCount + dynamicMemberCount);
        consensusChannelUri.put(CommonContext.ENDPOINT_PARAM_NAME, backupStatusEndpoint);

        context.clusterBackupContext
            .errorHandler(ClusterTests.errorHandler(index))
            .clusterConsensusEndpoints(clusterConsensusEndpoints)
            .consensusChannel(consensusChannelUri.toString())
            .clusterBackupCoolDownIntervalNs(TimeUnit.SECONDS.toNanos(1))
            .catchupEndpoint(clusterBackupCatchupEndpoint(0, staticMemberCount + dynamicMemberCount))
            .clusterDir(new File(baseDirName, "cluster-backup"))
            .archiveContext(context.aeronArchiveContext.clone())
            .deleteDirOnStart(cleanStart);

        backupNode = new TestBackupNode(context);

        return backupNode;
    }

    TestNode startStaticNodeFromBackup()
    {
        return startStaticNodeFromBackup(TestNode.TestService::new);
    }

    TestNode startStaticNodeFromBackup(final Supplier<? extends TestNode.TestService> serviceSupplier)
    {
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + backupNodeIndex;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + backupNodeIndex + "-driver";
        final TestNode.Context context = new TestNode.Context(serviceSupplier.get().index(backupNodeIndex));

        if (null == backupNode || !backupNode.isClosed())
        {
            throw new IllegalStateException("backup node must be closed before starting from backup");
        }

        context.aeronArchiveContext
            .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, backupNodeIndex))
            .controlRequestStreamId(100)
            .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, backupNodeIndex))
            .controlResponseStreamId(110 + backupNodeIndex)
            .aeronDirectoryName(aeronDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .errorHandler(ClusterTests.errorHandler(backupNodeIndex))
            .dirDeleteOnStart(true);

        context.archiveContext
            .maxCatalogEntries(MAX_CATALOG_ENTRIES)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .controlStreamId(context.aeronArchiveContext.controlRequestStreamId())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .localControlStreamId(context.aeronArchiveContext.controlRequestStreamId())
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(false);

        context.consensusModuleContext
            .errorHandler(ClusterTests.errorHandler(backupNodeIndex))
            .clusterMemberId(backupNodeIndex)
            .clusterMembers(singleNodeClusterMember(0, backupNodeIndex))
            .appointedLeaderId(backupNodeIndex)
            .clusterDir(new File(baseDirName, "cluster-backup"))
            .ingressChannel(INGRESS_CHANNEL)
            .logChannel(LOG_CHANNEL)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .deleteDirOnStart(false);

        context.serviceContainerContext
            .aeronDirectoryName(aeronDirName)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .clusterDir(new File(baseDirName, "service"))
            .clusteredService(context.service)
            .errorHandler(ClusterTests.errorHandler(backupNodeIndex));

        backupNode = null;
        nodes[backupNodeIndex] = new TestNode(context, dataCollector);

        return nodes[backupNodeIndex];
    }

    void stopNode(final TestNode testNode)
    {
        testNode.close();
    }

    void stopAllNodes()
    {
        CloseHelper.close(backupNode);
        CloseHelper.closeAll(nodes);
    }

    void restartAllNodes(final boolean cleanStart)
    {
        for (int i = 0; i < staticMemberCount; i++)
        {
            startStaticNode(i, cleanStart);
        }
    }

    void shouldPrintClientCloseReason(final boolean shouldPrintClientCloseReason)
    {
        this.shouldPrintClientCloseReason = shouldPrintClientCloseReason;
    }

    String staticClusterMembers()
    {
        return staticClusterMembers;
    }

    AeronCluster client()
    {
        return client;
    }

    ExpandableArrayBuffer msgBuffer()
    {
        return msgBuffer;
    }

    void reconnectClient()
    {
        if (null == client)
        {
            throw new IllegalStateException("Aeron client not previously connected");
        }

        client.close();

        client = AeronCluster.connect(
            new AeronCluster.Context()
                .egressListener(egressMessageListener)
                .ingressChannel(INGRESS_CHANNEL)
                .egressChannel(EGRESS_CHANNEL)
                .ingressEndpoints(staticClusterMemberEndpoints));
    }

    AeronCluster connectClient()
    {
        return connectClient(
            new AeronCluster.Context()
                .ingressChannel(INGRESS_CHANNEL)
                .egressChannel(EGRESS_CHANNEL));
    }

    AeronCluster connectClient(final AeronCluster.Context clientCtx)
    {
        final String aeronDirName = CommonContext.getAeronDirectoryName();

        if (null == clientMediaDriver)
        {
            dataCollector.add(Paths.get(aeronDirName));

            clientMediaDriver = MediaDriver.launch(
                new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(false)
                    .aeronDirectoryName(aeronDirName));
        }

        CloseHelper.close(client);
        client = AeronCluster.connect(
            clientCtx
                .aeronDirectoryName(aeronDirName)
                .egressListener(egressMessageListener)
                .ingressEndpoints(staticClusterMemberEndpoints));

        return client;
    }

    void closeClient()
    {
        CloseHelper.close(client);
    }

    void sendMessages(final int messageCount)
    {
        for (int i = 0; i < messageCount; i++)
        {
            msgBuffer.putInt(0, i);
            pollUntilMessageSent(BitUtil.SIZE_OF_INT);
        }
    }

    void sendUnexpectedMessages(final int messageCount)
    {
        final int length = msgBuffer.putStringWithoutLengthAscii(0, ClusterTests.UNEXPECTED_MSG);
        for (int i = 0; i < messageCount; i++)
        {
            pollUntilMessageSent(length);
        }
    }

    void pollUntilMessageSent(final int messageLength)
    {
        while (true)
        {
            client.pollEgress();

            final long result = client.offer(msgBuffer, 0, messageLength);
            if (result > 0)
            {
                return;
            }

            if (Publication.ADMIN_ACTION == result)
            {
                continue;
            }

            if (Publication.CLOSED == result)
            {
                throw new ClusterException("client publication is closed");
            }

            if (Publication.MAX_POSITION_EXCEEDED == result)
            {
                throw new ClusterException("max position exceeded");
            }

            Tests.sleep(1);
        }
    }

    void awaitResponseMessageCount(final int messageCount)
    {
        final EpochClock epochClock = client.context().aeron().context().epochClock();
        long heartbeatDeadlineMs = epochClock.time() + TimeUnit.SECONDS.toMillis(1);
        long count;

        while ((count = responseCount.get()) < messageCount)
        {
            Thread.yield();
            if (Thread.interrupted())
            {
                final String message = "count=" + count + " awaiting=" + messageCount;
                Tests.unexpectedInterruptStackTrace(message);
                fail(message);
            }

            client.pollEgress();

            final long nowMs = epochClock.time();
            if (nowMs > heartbeatDeadlineMs)
            {
                client.sendKeepAlive();
                heartbeatDeadlineMs = nowMs + TimeUnit.SECONDS.toMillis(1);
            }
        }
    }

    void awaitLeadershipEvent(final int count)
    {
        while (newLeaderEvent.get() < count)
        {
            Tests.sleep(1);
            client.pollEgress();
        }
    }

    void awaitCommitPosition(final TestNode node, final long logPosition)
    {
        while (node.commitPosition() < logPosition)
        {
            Tests.yield();
        }
    }

    void awaitActiveSessionCount(final TestNode node, final int count)
    {
        while (node.service().activeSessionCount() != count)
        {
            Tests.sleep(1);
        }
    }

    TestNode findLeader(final int skipIndex)
    {
        for (int i = 0; i < nodes.length; i++)
        {
            final TestNode node = nodes[i];
            if (i == skipIndex || null == node || node.isClosed())
            {
                continue;
            }

            if (node.isLeader() && ElectionState.CLOSED == node.electionState())
            {
                return node;
            }
        }

        return null;
    }

    TestNode findLeader()
    {
        return findLeader(NULL_VALUE);
    }

    TestNode awaitLeader(final int skipIndex)
    {
        TestNode leaderNode;
        while (null == (leaderNode = findLeader(skipIndex)))
        {
            Tests.sleep(10);
        }

        return leaderNode;
    }

    TestNode awaitLeader()
    {
        return awaitLeader(NULL_VALUE);
    }

    List<TestNode> followers()
    {
        final ArrayList<TestNode> followers = new ArrayList<>();

        for (final TestNode node : nodes)
        {
            if (null != node && !node.isClosed() && node.isFollower())
            {
                followers.add(node);
            }
        }

        return followers;
    }

    void awaitBackupState(final ClusterBackup.State targetState)
    {
        if (null == backupNode)
        {
            throw new IllegalStateException("no backup node present");
        }

        while (backupNode.backupState() != targetState)
        {
            Tests.sleep(10);
        }
    }

    void awaitBackupLiveLogPosition(final long position)
    {
        if (null == backupNode)
        {
            throw new IllegalStateException("no backup node present");
        }

        while (true)
        {
            final long livePosition = backupNode.liveLogPosition();
            if (livePosition >= position)
            {
                return;
            }

            if (NULL_POSITION == livePosition)
            {
                throw new ClusterException("backup live log position is closed");
            }

            Tests.sleep(10, "awaiting position=%d livePosition=%d", position, livePosition);
        }
    }

    TestNode node(final int index)
    {
        return nodes[index];
    }

    void takeSnapshot(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getControlToggle(leaderNode);
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));
    }

    void shutdownCluster(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getControlToggle(leaderNode);
        assertTrue(ClusterControl.ToggleState.SHUTDOWN.toggle(controlToggle));
    }

    void abortCluster(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getControlToggle(leaderNode);
        assertTrue(ClusterControl.ToggleState.ABORT.toggle(controlToggle));
    }

    void awaitSnapshotCount(final TestNode node, final long value)
    {
        final Counter snapshotCounter = node.consensusModule().context().snapshotCounter();
        while (snapshotCounter.get() < value)
        {
            Tests.yield();
            if (snapshotCounter.isClosed())
            {
                throw new IllegalStateException("counter is unexpectedly closed");
            }
        }
    }

    void awaitNodeTermination(final TestNode node)
    {
        while (!node.hasMemberTerminated() || !node.hasServiceTerminated())
        {
            Tests.yield();
        }
    }

    void awaitServicesMessageCount(final int messageCount)
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                awaitServiceMessageCount(node, messageCount);
            }
        }
    }

    void awaitServiceMessageCount(final TestNode node, final int messageCount)
    {
        final TestNode.TestService service = node.service();
        final EpochClock epochClock = client.context().aeron().context().epochClock();
        long keepAliveDeadlineMs = epochClock.time() + TimeUnit.SECONDS.toMillis(1);
        long count;

        while ((count = service.messageCount()) < messageCount)
        {
            Thread.yield();
            if (Thread.interrupted())
            {
                final String message = "count=" + count + " awaiting=" + messageCount;
                Tests.unexpectedInterruptStackTrace(message);
                fail(message);
            }

            if (service.hasReceivedUnexpectedMessage())
            {
                fail("service received unexpected message");
            }

            final long nowMs = epochClock.time();
            if (nowMs > keepAliveDeadlineMs)
            {
                client.sendKeepAlive();
                keepAliveDeadlineMs = nowMs + TimeUnit.SECONDS.toMillis(1);
            }
        }
    }

    void awaitSnapshotLoadedForService(final TestNode node)
    {
        while (!node.service().wasSnapshotLoaded())
        {
            Tests.yield();
        }
    }

    void awaitNeutralControlToggle(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getControlToggle(leaderNode);
        while (controlToggle.get() != ClusterControl.ToggleState.NEUTRAL.code())
        {
            Tests.yield();
        }
    }

    AtomicCounter getControlToggle(final TestNode leaderNode)
    {
        final CountersReader counters = leaderNode.countersReader();
        final int clusterId = leaderNode.consensusModule().context().clusterId();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters, clusterId);
        assertNotNull(controlToggle);

        return controlToggle;
    }

    static String memberSpecificPort(final String channel, final int memberId)
    {
        return channel.substring(0, channel.length() - 1) + memberId;
    }

    static String clusterMembers(final int clusterId, final int memberCount)
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < memberCount; i++)
        {
            builder
                .append(i).append(',')
                .append("localhost:2").append(clusterId).append("11").append(i).append(',')
                .append("localhost:2").append(clusterId).append("22").append(i).append(',')
                .append("localhost:2").append(clusterId).append("33").append(i).append(',')
                .append("localhost:2").append(clusterId).append("44").append(i).append(',')
                .append("localhost:801").append(i).append('|');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    static String singleNodeClusterMember(final int clusterId, final int i)
    {
        return i + "," +
            "localhost:2" + clusterId + "11" + i + ',' +
            "localhost:2" + clusterId + "22" + i + ',' +
            "localhost:2" + clusterId + "33" + i + ',' +
            "localhost:2" + clusterId + "44" + i + ',' +
            "localhost:801" + i;
    }

    static String ingressEndpoints(final int clusterId, final int memberCount)
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < memberCount; i++)
        {
            builder.append(i).append('=').append("localhost:2").append(clusterId).append("11").append(i).append(',');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    private static String[] clusterMembersEndpoints(final int clusterId, final int maxMemberCount)
    {
        final String[] clusterMembersEndpoints = new String[maxMemberCount];

        for (int i = 0; i < maxMemberCount; i++)
        {
            clusterMembersEndpoints[i] =
                "localhost:2" + clusterId + "11" + i + ',' +
                "localhost:2" + clusterId + "22" + i + ',' +
                "localhost:2" + clusterId + "33" + i + ',' +
                "localhost:2" + clusterId + "44" + i + ',' +
                "localhost:801" + i;
        }

        return clusterMembersEndpoints;
    }

    private static String clusterConsensusEndpoints(final int clusterId, final int staticMemberCount)
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < staticMemberCount; i++)
        {
            builder.append("localhost:2").append(clusterId).append("22").append(i).append(',');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    private static String clusterBackupStatusEndpoint(final int clusterId, final int maxMemberCount)
    {
        return "localhost:2" + clusterId + "22" + maxMemberCount;
    }

    private static String clusterBackupCatchupEndpoint(final int clusterId, final int maxMemberCount)
    {
        return "localhost:2" + clusterId + "44" + maxMemberCount;
    }

    public void invalidateLatestSnapshots()
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                try (RecordingLog recordingLog = new RecordingLog(node.consensusModule().context().clusterDir()))
                {
                    assertTrue(recordingLog.invalidateLatestSnapshot());
                }
            }
        }
    }

    public void dumpData(final TestInfo testInfo)
    {
        dataCollector.dumpData(testInfo);
    }

    static class ServiceContext
    {
        final Aeron.Context aeronCtx = new Aeron.Context();
        final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context();
        final ClusteredServiceContainer.Context serviceContainerCtx = new ClusteredServiceContainer.Context();
        TestNode.TestService service;
    }

    static class NodeContext
    {
        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context();
        final Archive.Context archiveCtx = new Archive.Context();
        final ConsensusModule.Context consensusModuleCtx = new ConsensusModule.Context();
        final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context();
    }

    static ServiceContext serviceContext(
        final int nodeIndex,
        final int serviceId,
        final NodeContext nodeCtx,
        final Supplier<? extends TestNode.TestService> serviceSupplier)
    {
        final int serviceIndex = 3 * nodeIndex + serviceId;
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + nodeIndex + "-" + serviceIndex;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + nodeIndex + "-driver";
        final ServiceContext serviceCtx = new ServiceContext();

        serviceCtx.service = serviceSupplier.get();

        serviceCtx.aeronCtx
            .aeronDirectoryName(aeronDirName)
            .awaitingIdleStrategy(YieldingIdleStrategy.INSTANCE);

        serviceCtx.aeronArchiveCtx
            .controlRequestChannel(nodeCtx.archiveCtx.localControlChannel())
            .controlRequestStreamId(nodeCtx.archiveCtx.localControlStreamId())
            .controlResponseChannel(nodeCtx.archiveCtx.localControlChannel())
            .controlResponseStreamId(1100 + serviceIndex)
            .recordingEventsChannel(nodeCtx.archiveCtx.recordingEventsChannel());

        serviceCtx.serviceContainerCtx
            .archiveContext(serviceCtx.aeronArchiveCtx.clone())
            .clusterDir(new File(baseDirName, "service"))
            .clusteredService(serviceCtx.service)
            .serviceId(serviceId)
            .snapshotChannel(SNAPSHOT_CHANNEL_DEFAULT + "|term-length=64k")
            .errorHandler(ClusterTests.errorHandler(serviceIndex));

        return serviceCtx;
    }

    static NodeContext nodeContext(final int index, final boolean cleanStart)
    {
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final NodeContext nodeCtx = new NodeContext();

        nodeCtx.mediaDriverCtx
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .errorHandler(ClusterTests.errorHandler(index))
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false);

        nodeCtx.archiveCtx
            .maxCatalogEntries(MAX_CATALOG_ENTRIES)
            .segmentFileLength(256 * 1024)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, index))
            .controlStreamId(100)
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .recordingEventsEnabled(false)
            .localControlStreamId(100)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(cleanStart);

        nodeCtx.aeronArchiveCtx
            .controlRequestChannel(nodeCtx.archiveCtx.localControlChannel())
            .controlRequestStreamId(nodeCtx.archiveCtx.localControlStreamId())
            .controlResponseChannel(nodeCtx.archiveCtx.localControlChannel())
            .controlResponseStreamId(110 + index)
            .recordingEventsChannel(nodeCtx.archiveCtx.recordingEventsChannel())
            .aeronDirectoryName(aeronDirName);

        nodeCtx.consensusModuleCtx
            .errorHandler(ClusterTests.errorHandler(index))
            .clusterMemberId(index)
            .clusterMembers(clusterMembers(0, 3))
            .serviceCount(2)
            .appointedLeaderId(NULL_VALUE)
            .clusterDir(new File(baseDirName, "consensus-module"))
            .ingressChannel("aeron:udp?term-length=64k")
            .logChannel(LOG_CHANNEL)
            .archiveContext(nodeCtx.aeronArchiveCtx.clone())
            .snapshotChannel(SNAPSHOT_CHANNEL_DEFAULT + "|term-length=64k")
            .deleteDirOnStart(cleanStart);

        return nodeCtx;
    }
}

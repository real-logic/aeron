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
package io.aeron.test.cluster;

import io.aeron.*;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.*;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.client.ControlledEgressListener;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventDecoder;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.RegistrationException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.Header;
import io.aeron.samples.archive.RecordingDescriptor;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import io.aeron.security.AuthorisationServiceSupplier;
import io.aeron.test.DataCollector;
import io.aeron.test.Tests;
import io.aeron.test.driver.DriverOutputConsumer;
import io.aeron.test.driver.RedirectingNameResolver;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ConsensusModule.Configuration.SNAPSHOT_CHANNEL_DEFAULT;
import static io.aeron.test.cluster.ClusterTests.LARGE_MSG;
import static io.aeron.test.cluster.ClusterTests.errorHandler;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

public class TestCluster implements AutoCloseable
{
    private static final int SEGMENT_FILE_LENGTH = 16 * 1024 * 1024;
    private static final long CATALOG_CAPACITY = 128 * 1024;
    private static final String LOG_CHANNEL = "aeron:udp?term-length=512k";
    // Use for testing cluster with multicast
    // TODO: Parameterise tests with this.
    // private static final String LOG_CHANNEL =
    //     "aeron:udp?term-length=512k|endpoint=224.20.30.39:24326|interface=localhost";
    private static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String ARCHIVE_LOCAL_CONTROL_CHANNEL = "aeron:ipc";
    private static final String EGRESS_CHANNEL = "aeron:udp?term-length=128k|endpoint=localhost:0";
    private static final String INGRESS_CHANNEL = "aeron:udp?term-length=128k|alias=ingress";
    private static final long STARTUP_CANVASS_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    public static final String DEFAULT_NODE_MAPPINGS =
        "node0,localhost,localhost|" +
        "node1,localhost,localhost|" +
        "node2,localhost,localhost|";

    private final DataCollector dataCollector = new DataCollector();
    private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
    private final MutableLong responseCount = new MutableLong();
    private final MutableInteger newLeaderEvent = new MutableInteger();
    private EgressListener egressListener = new EgressListener()
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
            else if (EventCode.CLOSED == code && shouldErrorOnClientClose)
            {
                final String msg = "[" + System.nanoTime() / 1_000_000_000.0 + "] session closed due to " + detail;
                throw new ClusterException(msg);
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
    private ControlledEgressListener controlledEgressListener;

    private final TestNode[] nodes;
    private final String staticClusterMembers;
    private final String staticClusterMemberEndpoints;
    private final String[] clusterMembersEndpoints;
    private final String clusterConsensusEndpoints;
    private final int staticMemberCount;
    private final int dynamicMemberCount;
    private final int appointedLeaderId;
    private final int backupNodeIndex;
    private final IntHashSet invalidInitialResolutions;
    private final IntFunction<TestNode.TestService[]> serviceSupplier;
    private boolean shouldErrorOnClientClose = true;
    private String logChannel;
    private String ingressChannel;
    private String egressChannel;
    private AuthorisationServiceSupplier authorisationServiceSupplier;

    private TestMediaDriver clientMediaDriver;
    private AeronCluster client;
    private TestBackupNode backupNode;

    TestCluster(
        final int staticMemberCount,
        final int dynamicMemberCount,
        final int appointedLeaderId,
        final IntHashSet invalidInitialResolutions,
        final IntFunction<TestNode.TestService[]> serviceSupplier)
    {
        this.serviceSupplier = Objects.requireNonNull(serviceSupplier);
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
        this.clusterConsensusEndpoints = clusterConsensusEndpoints(0, 0, staticMemberCount);
        this.staticMemberCount = staticMemberCount;
        this.dynamicMemberCount = dynamicMemberCount;
        this.appointedLeaderId = appointedLeaderId;
        this.invalidInitialResolutions = invalidInitialResolutions;
    }

    public static void awaitElectionClosed(final TestNode follower)
    {
        awaitElectionState(follower, ElectionState.CLOSED);
    }

    public static void awaitElectionState(final TestNode node, final ElectionState electionState)
    {
        while (node.electionState() != electionState)
        {
            await(10);
        }
    }

    private static void await(final int delayMs)
    {
        Tests.sleep(delayMs);
        ClusterTests.failOnClusterError();
    }

    private void await(final int delayMs, final Supplier<String> message)
    {
        Tests.sleep(delayMs, message);
        ClusterTests.failOnClusterError();
    }

    public static ClusterMembership awaitMembershipSize(final TestNode leader, final int size)
    {
        while (true)
        {
            final ClusterMembership clusterMembership = leader.clusterMembership();
            if (clusterMembership.activeMembers.size() == size)
            {
                return clusterMembership;
            }
            await(10);
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
                () -> CloseHelper.closeAll(Stream.of(nodes).filter(Objects::nonNull).collect(toList())),
                null != backupNode ? () -> backupNode.close() : null);
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

    public TestNode startStaticNode(final int index, final boolean cleanStart)
    {
        return startStaticNode(index, cleanStart, serviceSupplier);
    }

    public TestNode startStaticNode(
        final int index, final boolean cleanStart, final IntFunction<TestNode.TestService[]> serviceSupplier)
    {
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final TestNode.Context context = new TestNode.Context(serviceSupplier.apply(index), nodeNameMappings());

        context.aeronArchiveContext
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(archiveControlRequestChannel(index))
            .controlResponseChannel(archiveControlResponseChannel(index))
            .aeronDirectoryName(aeronDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .dirDeleteOnShutdown(false)
            .dirDeleteOnStart(true);

        context.archiveContext
            .catalogCapacity(CATALOG_CAPACITY)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .recordingEventsEnabled(false)
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(cleanStart);

        context.consensusModuleContext
            .clusterMemberId(index)
            .clusterMembers(staticClusterMembers)
            .startupCanvassTimeoutNs(STARTUP_CANVASS_TIMEOUT_NS)
            .appointedLeaderId(appointedLeaderId)
            .clusterDir(new File(baseDirName, "consensus-module"))
            .ingressChannel(ingressChannel)
            .logChannel(logChannel)
            .replicationChannel(REPLICATION_CHANNEL)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .authorisationServiceSupplier(authorisationServiceSupplier)
            .deleteDirOnStart(cleanStart);

        nodes[index] = new TestNode(context, dataCollector);

        return nodes[index];
    }

    public TestNode startDynamicNode(final int index, final boolean cleanStart)
    {
        return startDynamicNode(index, cleanStart, serviceSupplier);
    }

    public TestNode startDynamicNode(
        final int index, final boolean cleanStart, final IntFunction<TestNode.TestService[]> serviceSupplier)
    {
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final TestNode.Context context = new TestNode.Context(serviceSupplier.apply(index), nodeNameMappings());

        context.aeronArchiveContext
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(archiveControlRequestChannel(index))
            .controlResponseChannel(archiveControlResponseChannel(index))
            .aeronDirectoryName(aeronDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false);

        context.archiveContext
            .catalogCapacity(CATALOG_CAPACITY)
            .segmentFileLength(SEGMENT_FILE_LENGTH)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(cleanStart);

        context.consensusModuleContext
            .clusterMemberId(NULL_VALUE)
            .clusterMembers("")
            .clusterConsensusEndpoints(clusterConsensusEndpoints)
            .memberEndpoints(clusterMembersEndpoints[index])
            .clusterDir(new File(baseDirName, "consensus-module"))
            .ingressChannel(ingressChannel)
            .logChannel(logChannel)
            .replicationChannel(REPLICATION_CHANNEL)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .authorisationServiceSupplier(authorisationServiceSupplier)
            .deleteDirOnStart(cleanStart);

        nodes[index] = new TestNode(context, dataCollector);

        return nodes[index];
    }

    public TestNode startDynamicNodeConsensusEndpoints(final int index, final boolean cleanStart)
    {
        return startDynamicNodeConsensusEndpoints(index, cleanStart, serviceSupplier);
    }

    public TestNode startDynamicNodeConsensusEndpoints(
        final int index, final boolean cleanStart, final IntFunction<TestNode.TestService[]> serviceSupplier)
    {
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final TestNode.Context context = new TestNode.Context(serviceSupplier.apply(index), nodeNameMappings());

        context.aeronArchiveContext
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(archiveControlRequestChannel(index))
            .controlResponseChannel(archiveControlResponseChannel(index))
            .aeronDirectoryName(aeronDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false);

        context.archiveContext
            .catalogCapacity(CATALOG_CAPACITY)
            .segmentFileLength(SEGMENT_FILE_LENGTH)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(cleanStart);

        final String dynamicOnlyConsensusEndpoints = clusterConsensusEndpoints(0, 3, index);

        context.consensusModuleContext
            .clusterMemberId(NULL_VALUE)
            .clusterMembers("")
            .clusterConsensusEndpoints(dynamicOnlyConsensusEndpoints)
            .memberEndpoints(clusterMembersEndpoints[index])
            .clusterDir(new File(baseDirName, "consensus-module"))
            .ingressChannel(ingressChannel)
            .logChannel(logChannel)
            .replicationChannel(REPLICATION_CHANNEL)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .authorisationServiceSupplier(authorisationServiceSupplier)
            .deleteDirOnStart(cleanStart);

        nodes[index] = new TestNode(context, dataCollector);

        return nodes[index];
    }

    public TestNode startStaticNodeFromDynamicNode(final int index)
    {
        return startStaticNodeFromDynamicNode(index, serviceSupplier);
    }

    public TestNode startStaticNodeFromDynamicNode(
        final int index, final IntFunction<TestNode.TestService[]> serviceSupplier)
    {
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final TestNode.Context context = new TestNode.Context(serviceSupplier.apply(index), nodeNameMappings());

        context.aeronArchiveContext
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(archiveControlRequestChannel(index))
            .controlResponseChannel(archiveControlResponseChannel(index))
            .aeronDirectoryName(aeronDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .dirDeleteOnShutdown(false)
            .dirDeleteOnStart(true);

        context.archiveContext
            .catalogCapacity(CATALOG_CAPACITY)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .recordingEventsEnabled(false)
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(false);

        context.consensusModuleContext
            .clusterMemberId(index)
            .clusterMembers(clusterMembers(0, staticMemberCount + 1))
            .startupCanvassTimeoutNs(STARTUP_CANVASS_TIMEOUT_NS)
            .appointedLeaderId(appointedLeaderId)
            .clusterDir(new File(baseDirName, "consensus-module"))
            .ingressChannel(ingressChannel)
            .logChannel(logChannel)
            .replicationChannel(REPLICATION_CHANNEL)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .authorisationServiceSupplier(authorisationServiceSupplier)
            .deleteDirOnStart(false);

        nodes[index] = new TestNode(context, dataCollector);

        return nodes[index];
    }

    public TestBackupNode startClusterBackupNode(final boolean cleanStart)
    {
        final int index = staticMemberCount + dynamicMemberCount;
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final TestBackupNode.Context context = new TestBackupNode.Context();

        context.aeronArchiveContext
            .controlRequestChannel(archiveControlRequestChannel(index))
            .controlResponseChannel(archiveControlResponseChannel(index))
            .aeronDirectoryName(aeronDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .errorHandler(errorHandler(index))
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false)
            .nameResolver(new RedirectingNameResolver(nodeNameMappings()));

        context.archiveContext
            .catalogCapacity(CATALOG_CAPACITY)
            .segmentFileLength(SEGMENT_FILE_LENGTH)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(cleanStart);

        final ChannelUri consensusChannelUri = ChannelUri.parse(context.clusterBackupContext.consensusChannel());
        final String backupStatusEndpoint = clusterBackupStatusEndpoint(0, index);
        consensusChannelUri.put(CommonContext.ENDPOINT_PARAM_NAME, backupStatusEndpoint);

        final AeronArchive.Context clusterArchiveContext = new AeronArchive.Context();
        final ChannelUri clusterArchiveResponseChannel =
            ChannelUri.parse(clusterArchiveContext.controlResponseChannel());
        clusterArchiveResponseChannel.put(CommonContext.ENDPOINT_PARAM_NAME, hostname(index) + ":0");
        clusterArchiveContext.controlResponseChannel(clusterArchiveResponseChannel.toString());

        context.clusterBackupContext
            .errorHandler(errorHandler(index))
            .clusterConsensusEndpoints(clusterConsensusEndpoints)
            .consensusChannel(consensusChannelUri.toString())
            .clusterBackupCoolDownIntervalNs(TimeUnit.SECONDS.toNanos(1))
            .catchupEndpoint(hostname(index) + ":0")
            .clusterArchiveContext(clusterArchiveContext)
            .clusterDir(new File(baseDirName, "cluster-backup"))
            .deleteDirOnStart(cleanStart);

        backupNode = new TestBackupNode(context, dataCollector);

        return backupNode;
    }

    public TestNode startStaticNodeFromBackup()
    {
        return startStaticNodeFromBackup(serviceSupplier);
    }

    public TestNode startStaticNodeFromBackup(final IntFunction<TestNode.TestService[]> serviceSupplier)
    {
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + backupNodeIndex;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + backupNodeIndex + "-driver";
        final TestNode.Context context = new TestNode.Context(
            serviceSupplier.apply(backupNodeIndex),
            nodeNameMappings());

        if (null == backupNode || !backupNode.isClosed())
        {
            throw new IllegalStateException("backup node must be closed before starting from backup");
        }

        context.aeronArchiveContext
            .controlRequestChannel(archiveControlRequestChannel(backupNodeIndex))
            .controlResponseChannel(archiveControlResponseChannel(backupNodeIndex))
            .aeronDirectoryName(aeronDirName);

        context.mediaDriverContext
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .dirDeleteOnStart(true);

        context.archiveContext
            .catalogCapacity(CATALOG_CAPACITY)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(context.aeronArchiveContext.controlRequestChannel())
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(false);

        context.consensusModuleContext
            .clusterMemberId(backupNodeIndex)
            .clusterMembers(singleNodeClusterMember(0, backupNodeIndex))
            .appointedLeaderId(backupNodeIndex)
            .clusterDir(new File(baseDirName, "cluster-backup"))
            .ingressChannel(ingressChannel)
            .logChannel(logChannel)
            .replicationChannel(REPLICATION_CHANNEL)
            .archiveContext(context.aeronArchiveContext.clone()
                .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
                .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .authorisationServiceSupplier(authorisationServiceSupplier)
            .deleteDirOnStart(false);

        backupNode = null;
        nodes[backupNodeIndex] = new TestNode(context, dataCollector);

        return nodes[backupNodeIndex];
    }

    public void stopNode(final TestNode testNode)
    {
        testNode.close();
    }

    public void stopAllNodes()
    {
        CloseHelper.close(backupNode);
        CloseHelper.closeAll(nodes);
    }

    public void restartAllNodes(final boolean cleanStart)
    {
        for (int i = 0; i < staticMemberCount; i++)
        {
            startStaticNode(i, cleanStart);
        }
    }

    public void shouldErrorOnClientClose(final boolean shouldErrorOnClose)
    {
        this.shouldErrorOnClientClose = shouldErrorOnClose;
    }

    public void logChannel(final String logChannel)
    {
        this.logChannel = logChannel;
    }

    public void ingressChannel(final String ingressChannel)
    {
        this.ingressChannel = ingressChannel;
    }

    public void egressChannel(final String egressChannel)
    {
        this.egressChannel = egressChannel;
    }

    public void egressListener(final EgressListener egressListener)
    {
        this.egressListener = egressListener;
    }

    public void controlledEgressListener(final ControlledEgressListener controlledEgressListener)
    {
        this.controlledEgressListener = controlledEgressListener;
    }

    public void authorisationServiceSupplier(final AuthorisationServiceSupplier authorisationServiceSupplier)
    {
        this.authorisationServiceSupplier = authorisationServiceSupplier;
    }

    public String staticClusterMembers()
    {
        return staticClusterMembers;
    }

    public AeronCluster client()
    {
        return client;
    }

    public ExpandableArrayBuffer msgBuffer()
    {
        return msgBuffer;
    }

    public AeronCluster reconnectClient()
    {
        if (null == client)
        {
            throw new IllegalStateException("Aeron client not previously connected");
        }

        return connectClient();
    }

    public AeronCluster connectClient()
    {
        return connectClient(new AeronCluster.Context().ingressChannel(ingressChannel).egressChannel(egressChannel));
    }

    public AeronCluster connectClient(final AeronCluster.Context clientCtx)
    {
        final String aeronDirName = CommonContext.getAeronDirectoryName();

        if (null == clientMediaDriver)
        {
            dataCollector.add(Paths.get(aeronDirName));

            final MediaDriver.Context ctx = new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(false)
                .aeronDirectoryName(aeronDirName)
                .nameResolver(new RedirectingNameResolver(nodeNameMappings()));

            clientMediaDriver = TestMediaDriver.launch(ctx, clientDriverOutputConsumer(dataCollector));
        }

        clientCtx
            .aeronDirectoryName(aeronDirName)
            .isIngressExclusive(true)
            .egressListener(egressListener)
            .controlledEgressListener(controlledEgressListener)
            .ingressEndpoints(staticClusterMemberEndpoints);

        try
        {
            CloseHelper.close(client);
            client = AeronCluster.connect(clientCtx.clone());
        }
        catch (final TimeoutException ex)
        {
            System.out.println("Warning: " + ex);

            CloseHelper.close(client);
            client = AeronCluster.connect(clientCtx);
        }

        return client;
    }

    public AeronCluster connectIpcClient(final AeronCluster.Context clientCtx, final String aeronDirName)
    {

        clientCtx
            .aeronDirectoryName(aeronDirName)
            .isIngressExclusive(true)
            .ingressChannel("aeron:ipc")
            .egressChannel("aeron:ipc")
            .egressListener(egressListener)
            .controlledEgressListener(controlledEgressListener)
            .ingressEndpoints(null);

        try
        {
            CloseHelper.close(client);
            client = AeronCluster.connect(clientCtx.clone());
        }
        catch (final TimeoutException ex)
        {
            System.out.println("Warning: " + ex);

            CloseHelper.close(client);
            client = AeronCluster.connect(clientCtx);
        }

        return client;
    }

    public void closeClient()
    {
        CloseHelper.close(client);
    }

    public void sendMessages(final int messageCount)
    {
        for (int i = 0; i < messageCount; i++)
        {
            msgBuffer.putInt(0, i);
            try
            {
                pollUntilMessageSent(BitUtil.SIZE_OF_INT);
            }
            catch (final Exception ex)
            {
                throw new ClusterException("failed to send message " + i + " of " + messageCount, ex);
            }
        }
    }

    public void sendLargeMessages(final int messageCount)
    {
        for (int i = 0; i < messageCount; i++)
        {
            msgBuffer.putStringWithoutLengthAscii(0, LARGE_MSG);
            try
            {
                pollUntilMessageSent(LARGE_MSG.length());
            }
            catch (final Exception ex)
            {
                throw new ClusterException("failed to send message " + i + " of " + messageCount, ex);
            }
        }
    }

    public void sendUnexpectedMessages(final int messageCount)
    {
        final int length = msgBuffer.putStringWithoutLengthAscii(0, ClusterTests.UNEXPECTED_MSG);
        for (int i = 0; i < messageCount; i++)
        {
            try
            {
                pollUntilMessageSent(length);
            }
            catch (final Exception ex)
            {
                throw new ClusterException("failed to send message " + i + " of " + messageCount, ex);
            }
        }
    }

    public void pollUntilMessageSent(final int messageLength)
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

            await(1);
        }
    }

    public void awaitResponseMessageCount(final int messageCount)
    {
        final EpochClock epochClock = client.context().aeron().context().epochClock();
        long heartbeatDeadlineMs = epochClock.time() + TimeUnit.SECONDS.toMillis(1);
        long count;
        final Supplier<String> msg = () -> "expected=" + messageCount + " responseCount=" + responseCount.get();

        while ((count = responseCount.get()) < messageCount)
        {
            client.pollEgress();

            final long nowMs = epochClock.time();
            if (nowMs > heartbeatDeadlineMs)
            {
                try
                {
                    client.sendKeepAlive();
                }
                catch (final ClusterException e)
                {
                    throw new RuntimeException("count=" + count + " awaiting=" + messageCount, e);
                }
                heartbeatDeadlineMs = nowMs + TimeUnit.SECONDS.toMillis(1);
            }

            Tests.yieldingIdle(msg);
        }
    }

    public void awaitNewLeadershipEvent(final int count)
    {
        while (newLeaderEvent.get() < count || !client.ingressPublication().isConnected())
        {
            await(1);
            client.pollEgress();
        }
    }

    public void awaitCommitPosition(final TestNode node, final long logPosition)
    {
        while (node.commitPosition() < logPosition)
        {
            Tests.yield();
        }
    }

    public void awaitActiveSessionCount(final TestNode node, final int count)
    {
        final Supplier<String> message =
            () -> "node " + node + " fail to reach active session count, expected=" + count + ", current=" +
            node.service().activeSessionCount();

        while (node.service().activeSessionCount() != count)
        {
            await(1, message);
        }
    }

    public void awaitActiveSessionCount(final int count)
    {
        for (final TestNode node : nodes)
        {
            if (null != node && !node.isClosed())
            {
                awaitActiveSessionCount(node, count);
            }
        }
    }

    public TestNode findLeader(final int skipIndex)
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

    public TestNode findLeader()
    {
        return findLeader(NULL_VALUE);
    }

    public TestNode awaitLeader(final int skipIndex)
    {
        final Supplier<String> message = () -> Arrays.stream(nodes)
            .map((node) -> null != node ? node.index() + " " + node.role() + " " + node.electionState() : "null")
            .collect(Collectors.joining(", "));

        TestNode leaderNode;
        while (null == (leaderNode = findLeader(skipIndex)))
        {
            await(10, message);
        }

        return leaderNode;
    }

    public TestNode awaitLeader()
    {
        return awaitLeader(NULL_VALUE);
    }

    public List<TestNode> followers()
    {
        return followers(0);
    }

    public ArrayList<TestNode> followers(final int expectedMinimumFollowerCount)
    {
        final ArrayList<TestNode> followers = new ArrayList<>();
        final EnumMap<Cluster.Role, ArrayList<TestNode>> nonFollowers = new EnumMap<>(Cluster.Role.class);

        for (final TestNode node : nodes)
        {
            if (null != node && !node.isClosed())
            {
                while (ElectionState.CLOSED != node.electionState())
                {
                    Tests.yield();
                }

                final Cluster.Role role = node.role();
                if (role == Cluster.Role.FOLLOWER)
                {
                    followers.add(node);
                }
                else
                {
                    nonFollowers.computeIfAbsent(role, (r) -> new ArrayList<>()).add(node);
                }
            }
        }

        if (followers.size() < expectedMinimumFollowerCount)
        {
            throw new RuntimeException(
                "expectedMinimumFollowerCount=" + expectedMinimumFollowerCount +
                " < followers.size=" + followers.size() +
                " nonFollowers=" + nonFollowers);
        }

        return followers;
    }

    public void awaitBackupState(final ClusterBackup.State targetState)
    {
        if (null == backupNode)
        {
            throw new IllegalStateException("no backup node present");
        }

        while (true)
        {
            final ClusterBackup.State state = backupNode.backupState();
            if (targetState == state)
            {
                break;
            }

            if (ClusterBackup.State.CLOSED == state)
            {
                throw new IllegalStateException("backup is closed");
            }

            await(10);
        }
    }

    public void awaitBackupLiveLogPosition(final long position)
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
                break;
            }

            if (NULL_POSITION == livePosition)
            {
                throw new ClusterException("backup live log position is closed");
            }

            Tests.sleep(10, "awaiting position=%d livePosition=%d", position, livePosition);
        }
    }

    public TestNode node(final int index)
    {
        return nodes[index];
    }

    public void takeSnapshot(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getControlToggle(leaderNode);
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));
    }

    public void shutdownCluster(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getControlToggle(leaderNode);
        assertTrue(ClusterControl.ToggleState.SHUTDOWN.toggle(controlToggle));
    }

    public void abortCluster(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getControlToggle(leaderNode);
        assertTrue(ClusterControl.ToggleState.ABORT.toggle(controlToggle));
    }

    public void awaitSnapshotCount(final long value)
    {
        for (final TestNode node : nodes)
        {
            if (null != node && !node.isClosed())
            {
                awaitSnapshotCount(node, value);
            }
        }
    }

    public void awaitSnapshotCount(final TestNode node, final long value)
    {
        final Counter snapshotCounter = node.consensusModule().context().snapshotCounter();
        final Supplier<String> msg =
            () -> "Node=" + node.index() + " expected=" + value + " snapshotCount=" + snapshotCounter.get();
        while (true)
        {
            if (snapshotCounter.isClosed())
            {
                throw new IllegalStateException("counter is unexpectedly closed");
            }

            if (snapshotCounter.get() >= value)
            {
                break;
            }

            Tests.yieldingIdle(msg);
        }
    }

    public long getSnapshotCount(final TestNode node)
    {
        final Counter snapshotCounter = node.consensusModule().context().snapshotCounter();

        if (snapshotCounter.isClosed())
        {
            throw new IllegalStateException("counter is unexpectedly closed");
        }

        return snapshotCounter.get();
    }


    public void awaitNodeTermination(final TestNode node)
    {
        while (!node.hasMemberTerminated() || !node.hasServiceTerminated())
        {
            Tests.yield();
        }
    }

    public void awaitNodeTerminations()
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                awaitNodeTermination(node);
            }
        }
    }

    public void awaitServicesMessageCount(final int messageCount)
    {
        for (final TestNode node : nodes)
        {
            if (null != node && !node.isClosed())
            {
                awaitServiceMessageCount(node, messageCount);
            }
        }
    }

    public void terminationsExpected(final boolean isExpected)
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                node.isTerminationExpected(isExpected);
            }
        }
    }

    public void awaitServiceMessageCount(final TestNode node, final int messageCount)
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
                throw new TimeoutException("count=" + count + " awaiting=" + messageCount + " node=" + node);
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

    public void awaitServiceState(final TestNode node, final Predicate<TestNode> predicate)
    {
        final EpochClock epochClock = client.context().aeron().context().epochClock();
        long keepAliveDeadlineMs = epochClock.time() + TimeUnit.SECONDS.toMillis(1);

        while (!predicate.test(node))
        {
            Thread.yield();
            if (Thread.interrupted())
            {
                throw new TimeoutException("timeout while awaiting condition");
            }

            final long nowMs = epochClock.time();
            if (nowMs > keepAliveDeadlineMs)
            {
                client.sendKeepAlive();
                keepAliveDeadlineMs = nowMs + TimeUnit.SECONDS.toMillis(1);
            }
        }
    }

    public void awaitSnapshotsLoaded()
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                awaitSnapshotLoadedForService(node);
            }
        }
    }

    public void awaitSnapshotLoadedForService(final TestNode node)
    {
        while (!node.allSnapshotsLoaded())
        {
            Tests.yield();
        }
    }

    public void awaitNeutralControlToggle(final TestNode leaderNode)
    {
        final AtomicCounter controlToggle = getControlToggle(leaderNode);
        while (controlToggle.get() != ClusterControl.ToggleState.NEUTRAL.code())
        {
            Tests.yield();
        }
    }

    public AtomicCounter getControlToggle(final TestNode leaderNode)
    {
        final CountersReader counters = leaderNode.countersReader();
        final int clusterId = leaderNode.consensusModule().context().clusterId();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters, clusterId);
        assertNotNull(controlToggle);

        return controlToggle;
    }

    public static String clusterMembers(final int clusterId, final int memberCount)
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < memberCount; i++)
        {
            builder
                .append(i).append(',')
                .append(hostname(i)).append(":2").append(clusterId).append("11").append(i).append(',')
                .append(hostname(i)).append(":2").append(clusterId).append("22").append(i).append(',')
                .append(hostname(i)).append(":2").append(clusterId).append("33").append(i).append(',')
                .append(hostname(i)).append(":0,")
                .append(hostname(i)).append(":801").append(i).append('|');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    public static String singleNodeClusterMember(final int clusterId, final int i)
    {
        final String hostname = hostname(i);

        return i + "," +
            hostname + ":2" + clusterId + "11" + i + ',' +
            hostname + ":2" + clusterId + "22" + i + ',' +
            hostname + ":2" + clusterId + "33" + i + ',' +
            hostname + ":0," +
            hostname + ":801" + i;
    }

    public static String ingressEndpoints(final int clusterId, final int memberCount)
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < memberCount; i++)
        {
            builder.append(i).append('=').append(hostname(i)).append(":2").append(clusterId).append("11")
                .append(i).append(',');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    public void purgeLogToLastSnapshot()
    {
        for (final TestNode testNode : nodes)
        {
            purgeLogToLastSnapshot(testNode);
        }
    }

    public void purgeLogToLastSnapshot(final TestNode node)
    {
        if (null == node || node.isClosed())
        {
            return;
        }

        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);
        final RecordingLog recordingLog = new RecordingLog(node.consensusModule().context().clusterDir(), false);
        final RecordingLog.Entry latestSnapshot = Objects.requireNonNull(
            recordingLog.getLatestSnapshot(ConsensusModule.Configuration.SERVICE_ID));
        final long recordingId = recordingLog.findLastTermRecordingId();
        if (RecordingPos.NULL_RECORDING_ID == recordingId)
        {
            throw new RuntimeException("Unable to find log recording");
        }

        try (Aeron aeron = Aeron.connect(
            new Aeron.Context().aeronDirectoryName(node.mediaDriver().aeronDirectoryName())))
        {
            final AeronArchive.Context aeronArchiveCtx = node
                .consensusModule()
                .context()
                .archiveContext()
                .clone()
                .aeron(aeron).ownsAeronClient(false);

            try (AeronArchive aeronArchive = AeronArchive.connect(aeronArchiveCtx))
            {
                aeronArchive.listRecording(recordingId, collector.reset());
                final RecordingDescriptor recordingDescriptor = collector.descriptors().get(0);

                final long newStartPosition = AeronArchive.segmentFileBasePosition(
                    recordingDescriptor.startPosition(),
                    latestSnapshot.logPosition,
                    recordingDescriptor.termBufferLength(),
                    recordingDescriptor.segmentFileLength());
                aeronArchive.purgeSegments(recordingId, newStartPosition);
            }
        }
    }

    private static String[] clusterMembersEndpoints(final int clusterId, final int maxMemberCount)
    {
        final String[] clusterMembersEndpoints = new String[maxMemberCount];

        for (int i = 0; i < maxMemberCount; i++)
        {
            clusterMembersEndpoints[i] =
                hostname(i) + ":2" + clusterId + "11" + i + ',' +
                hostname(i) + ":2" + clusterId + "22" + i + ',' +
                hostname(i) + ":2" + clusterId + "33" + i + ',' +
                hostname(i) + ":0," +
                hostname(i) + ":801" + i;
        }

        return clusterMembersEndpoints;
    }

    private static String clusterConsensusEndpoints(final int clusterId, final int beginIndex, final int endIndex)
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = beginIndex; i < endIndex; i++)
        {
            builder.append(hostname(i)).append(":2").append(clusterId).append("22").append(i).append(',');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    static String hostname(final int memberId)
    {
        return memberId < 3 ? "node" + memberId : "localhost";
    }

    private static String clusterBackupStatusEndpoint(final int clusterId, final int maxMemberCount)
    {
        return hostname(maxMemberCount) + ":2" + clusterId + "22" + maxMemberCount;
    }

    private static String archiveControlRequestChannel(final int memberId)
    {
        return "aeron:udp?endpoint=" + hostname(memberId) + ":801" + memberId;
    }

    private static String archiveControlResponseChannel(final int memberId)
    {
        return "aeron:udp?endpoint=" + hostname(memberId) + ":0";
    }

    public void invalidateLatestSnapshot()
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                try (RecordingLog recordingLog = new RecordingLog(node.consensusModule().context().clusterDir(), false))
                {
                    assertTrue(recordingLog.invalidateLatestSnapshot());
                }
            }
        }
    }

    public void disableNameResolution(final String hostname)
    {
        toggleNameResolution(hostname, RedirectingNameResolver.DISABLE_RESOLUTION);
    }

    public void enableNameResolution(final String hostname)
    {
        toggleNameResolution(hostname, RedirectingNameResolver.USE_INITIAL_RESOLUTION_HOST);
    }

    public void restoreNameResolution(final int nodeId)
    {
        invalidInitialResolutions.remove(nodeId);
        toggleNameResolution(hostname(nodeId), RedirectingNameResolver.USE_RE_RESOLUTION_HOST);
    }

    private void toggleNameResolution(final String hostname, final int disableValue)
    {
        for (final TestNode node : nodes)
        {
            if (null != node)
            {
                final CountersReader counters = node.mediaDriver().counters();
                RedirectingNameResolver.updateNameResolutionStatus(counters, hostname, disableValue);
            }
        }
    }

    private String nodeNameMappings()
    {
        return nodeNameMappings(invalidInitialResolutions);
    }

    private static String nodeNameMappings(final IntHashSet invalidInitialResolutions)
    {
        return
            "node0," + (invalidInitialResolutions.contains(0) ? "bad.invalid" : "localhost") + ",localhost|" +
            "node1," + (invalidInitialResolutions.contains(1) ? "bad.invalid" : "localhost") + ",localhost|" +
            "node2," + (invalidInitialResolutions.contains(2) ? "bad.invalid" : "localhost") + ",localhost|";
    }

    public DataCollector dataCollector()
    {
        return dataCollector;
    }

    public void assertRecordingLogsEqual()
    {
        List<RecordingLog.Entry> firstEntries = null;
        for (final TestNode node : nodes)
        {
            if (null == node)
            {
                continue;
            }

            try (RecordingLog recordingLog = new RecordingLog(node.consensusModule().context().clusterDir(), false))
            {
                if (null == firstEntries)
                {
                    firstEntries = recordingLog.entries();
                }
                else
                {
                    final List<RecordingLog.Entry> entries = recordingLog.entries();
                    assertEquals(
                        firstEntries.size(),
                        entries.size(),
                        "length mismatch: \n[0]" + firstEntries + " != " + "\n[" + node.index() + "] " + entries);
                    for (int i = 0; i < firstEntries.size(); i++)
                    {
                        final RecordingLog.Entry a = firstEntries.get(i);
                        final RecordingLog.Entry b = entries.get(i);

                        final ReflectionEquals matcher = new ReflectionEquals(a, "timestamp");
                        assertTrue(matcher.matches(b), "Mismatch (" + i + "): " + a + " != " + b);
                    }
                }
            }
        }
    }

    public void validateRecordingLogWithReplay(final int nodeId)
    {
        final TestNode node = node(nodeId);
        final ConsensusModule.Context consensusModuleCtx = node.consensusModule().context();
        final AeronArchive.Context clone = consensusModuleCtx.archiveContext().clone();
        try (
            AeronArchive aeronArchive = AeronArchive.connect(clone);
            RecordingLog recordingLog = new RecordingLog(consensusModuleCtx.clusterDir(), false))
        {
            final RecordingLog.Entry lastTerm = recordingLog.findLastTerm();
            assertNotNull(lastTerm);
            final long recordingId = lastTerm.recordingId;

            final long recordingPosition = aeronArchive.getRecordingPosition(recordingId);
            final Subscription replay = aeronArchive.replay(
                recordingId,
                0,
                recordingPosition,
                "aeron:udp?endpoint=localhost:6666",
                100001);

            final MutableLong position = new MutableLong();

            final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
            final NewLeadershipTermEventDecoder newLeadershipTermEventDecoder = new NewLeadershipTermEventDecoder();

            while (position.get() < recordingPosition)
            {
                replay.poll(
                    (buffer, offset, length, header) ->
                    {
                        messageHeaderDecoder.wrap(buffer, offset);

                        if (NewLeadershipTermEventDecoder.TEMPLATE_ID == messageHeaderDecoder.templateId())
                        {
                            newLeadershipTermEventDecoder.wrapAndApplyHeader(buffer, offset, messageHeaderDecoder);

                            final RecordingLog.Entry termEntry = recordingLog.findTermEntry(
                                newLeadershipTermEventDecoder.leadershipTermId());

                            assertNotNull(termEntry);
                            assertEquals(
                                newLeadershipTermEventDecoder.termBaseLogPosition(), termEntry.termBaseLogPosition);

                            if (0 < newLeadershipTermEventDecoder.leadershipTermId())
                            {
                                final RecordingLog.Entry previousTermEntry = recordingLog.findTermEntry(
                                    newLeadershipTermEventDecoder.leadershipTermId() - 1);
                                assertNotNull(previousTermEntry);
                                assertEquals(
                                    newLeadershipTermEventDecoder.termBaseLogPosition(),
                                    previousTermEntry.logPosition,
                                    previousTermEntry.toString());
                            }
                        }

                        position.set(header.position());
                    },
                    10);
            }
        }
    }

    public static class ServiceContext
    {
        public final Aeron.Context aeronCtx = new Aeron.Context();
        public final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context();
        public final ClusteredServiceContainer.Context serviceContainerCtx = new ClusteredServiceContainer.Context();
        TestNode.TestService service;
    }

    public static class NodeContext
    {
        public final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context();
        public final Archive.Context archiveCtx = new Archive.Context();
        public final ConsensusModule.Context consensusModuleCtx = new ConsensusModule.Context()
            .replicationChannel("aeron:udp?endpoint=localhost:0");
        public final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context();
    }

    public static ServiceContext serviceContext(
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
            .controlResponseChannel(nodeCtx.archiveCtx.localControlChannel())
            .recordingEventsChannel(nodeCtx.archiveCtx.recordingEventsChannel());

        serviceCtx.serviceContainerCtx
            .archiveContext(serviceCtx.aeronArchiveCtx.clone())
            .clusterDir(new File(baseDirName, "service"))
            .clusteredService(serviceCtx.service)
            .serviceId(serviceId)
            .snapshotChannel(SNAPSHOT_CHANNEL_DEFAULT + "|term-length=64k");

        return serviceCtx;
    }

    public static NodeContext nodeContext(final int index, final boolean cleanStart)
    {
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final NodeContext nodeCtx = new NodeContext();

        nodeCtx.mediaDriverCtx
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false)
            .nameResolver(new RedirectingNameResolver(DEFAULT_NODE_MAPPINGS));

        nodeCtx.archiveCtx
            .catalogCapacity(CATALOG_CAPACITY)
            .segmentFileLength(256 * 1024)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(archiveControlRequestChannel(index))
            .localControlChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(cleanStart);

        nodeCtx.aeronArchiveCtx
            .controlRequestChannel(nodeCtx.archiveCtx.localControlChannel())
            .controlResponseChannel(nodeCtx.archiveCtx.localControlChannel())
            .recordingEventsChannel(nodeCtx.archiveCtx.recordingEventsChannel())
            .aeronDirectoryName(aeronDirName);

        nodeCtx.consensusModuleCtx
            .clusterMemberId(index)
            .clusterMembers(clusterMembers(0, 3))
            .startupCanvassTimeoutNs(STARTUP_CANVASS_TIMEOUT_NS)
            .serviceCount(2)
            .clusterDir(new File(baseDirName, "consensus-module"))
            .ingressChannel(INGRESS_CHANNEL)
            .logChannel(LOG_CHANNEL)
            .archiveContext(nodeCtx.aeronArchiveCtx.clone())
            .snapshotChannel(SNAPSHOT_CHANNEL_DEFAULT + "|term-length=64k")
            .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .deleteDirOnStart(cleanStart);

        return nodeCtx;
    }

    public static Builder aCluster()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private int nodeCount = 3;
        private int dynamicNodeCount = 0;
        private int appointedLeaderId = NULL_VALUE;
        private String logChannel = LOG_CHANNEL;
        private String ingressChannel = INGRESS_CHANNEL;
        private String egressChannel = EGRESS_CHANNEL;
        private AuthorisationServiceSupplier authorisationServiceSupplier;
        private IntFunction<TestNode.TestService[]> serviceSupplier =
            (i) -> new TestNode.TestService[]{ new TestNode.TestService().index(i) };
        private final IntHashSet invalidInitialResolutions = new IntHashSet();

        public Builder withStaticNodes(final int nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder withDynamicNodes(final int nodeCount)
        {
            this.dynamicNodeCount = nodeCount;
            return this;
        }

        public Builder withAppointedLeader(final int appointedLeaderId)
        {
            this.appointedLeaderId = appointedLeaderId;
            return this;
        }

        public Builder withInvalidNameResolution(final int nodeId)
        {
            if (2 < nodeId)
            {
                throw new IllegalArgumentException("Only nodes 0 to 2 have name mappings that can be invalidated");
            }

            invalidInitialResolutions.add(nodeId);
            return this;
        }

        public Builder withLogChannel(final String logChannel)
        {
            this.logChannel = logChannel;
            return this;
        }

        public Builder withIngressChannel(final String ingressChannel)
        {
            this.ingressChannel = ingressChannel;
            return this;
        }

        public Builder withEgressChannel(final String egressChannel)
        {
            this.egressChannel = egressChannel;
            return this;
        }

        public Builder withServiceSupplier(final IntFunction<TestNode.TestService[]> serviceSupplier)
        {
            this.serviceSupplier = serviceSupplier;
            return this;
        }

        public Builder withAuthorisationServiceSupplier(final AuthorisationServiceSupplier authorisationServiceSupplier)
        {
            this.authorisationServiceSupplier = authorisationServiceSupplier;
            return this;
        }

        public TestCluster start()
        {
            return start(nodeCount);
        }

        public TestCluster start(final int toStart)
        {
            if (toStart > nodeCount)
            {
                throw new IllegalStateException(
                    "Unable to start " + toStart + " nodes, only " + nodeCount + " available");
            }

            final TestCluster testCluster = new TestCluster(
                nodeCount,
                dynamicNodeCount,
                appointedLeaderId,
                invalidInitialResolutions,
                serviceSupplier);
            testCluster.logChannel(logChannel);
            testCluster.ingressChannel(ingressChannel);
            testCluster.egressChannel(egressChannel);
            testCluster.authorisationServiceSupplier(authorisationServiceSupplier);

            try
            {
                for (int i = 0; i < toStart; i++)
                {
                    try
                    {
                        testCluster.startStaticNode(i, true);
                    }
                    catch (final RegistrationException e)
                    {
                        if (!invalidInitialResolutions.contains(i))
                        {
                            throw e;
                        }
                    }
                }
            }
            catch (final Exception e)
            {
                CloseHelper.close(testCluster);
                throw e;
            }

            return testCluster;
        }
    }

    public static DriverOutputConsumer clientDriverOutputConsumer(final DataCollector dataCollector)
    {
        if (TestMediaDriver.shouldRunJavaMediaDriver())
        {
            return null;
        }

        return new DriverOutputConsumer()
        {
            public void outputFiles(
                final String aeronDirectoryName, final File stdoutFile, final File stderrFile)
            {
                dataCollector.add(stdoutFile.toPath());
                dataCollector.add(stderrFile.toPath());
            }
        };
    }
}

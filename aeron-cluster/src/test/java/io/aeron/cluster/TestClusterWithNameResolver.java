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

import static io.aeron.Aeron.NULL_VALUE;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.fail;

import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.TestNode.TestService;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.test.DataCollector;
import io.aeron.test.Tests;
import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NoOpLock;
import org.junit.jupiter.api.TestInfo;

public class TestClusterWithNameResolver implements AutoCloseable
{

    private static final long MAX_CATALOG_ENTRIES = 128;
    private static final String LOG_CHANNEL = "aeron:udp?term-length=512k";
    private static final String ARCHIVE_LOCAL_CONTROL_CHANNEL = "aeron:ipc?term-length=64k";
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
            else if (EventCode.CLOSED == code)
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
    private final TestNameResolver nameResolver = new TestNameResolver();

    private final TestNode[] nodes;
    private final String staticClusterMembers;
    private final String staticClusterMemberEndpoints;
    private final int appointedLeaderId;

    private MediaDriver clientMediaDriver;
    private AeronCluster client;

    TestClusterWithNameResolver(final int staticMemberCount, final int dynamicMemberCount,
        final int appointedLeaderId)
    {
        final int memberCount = staticMemberCount + dynamicMemberCount;
        if ((memberCount + 1) >= 10)
        {
            throw new IllegalArgumentException("max members exceeded: max=9 count=" + memberCount);
        }

        this.nodes = new TestNode[memberCount + 1];
        this.staticClusterMembers = clusterMembers(0, staticMemberCount);
        this.staticClusterMemberEndpoints = ingressEndpoints(0, staticMemberCount);
        this.appointedLeaderId = appointedLeaderId;
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
                () -> CloseHelper.closeAll(Stream.of(nodes).map(TestCluster::closeAndDeleteNode).collect(toList())));
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

    static TestClusterWithNameResolver startThreeNodeStaticCluster()
    {
        final TestClusterWithNameResolver testCluster = new TestClusterWithNameResolver(3, 0,
            NULL_VALUE);
        for (int i = 0; i < 3; i++)
        {
            testCluster.startStaticNode(i, TestNode.TestService::new);
        }

        return testCluster;
    }

    TestNode startStaticNode(
        final int index,
        final Supplier<? extends TestService> serviceSupplier)
    {
        nameResolver.populate("local-" + index, "localhost");
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
        final TestNode.Context context = new TestNode.Context(serviceSupplier.get().index(index));

        context.aeronArchiveContext
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(memberSpecificPort(archiveControlRequestChannel(index), index))
            .controlRequestStreamId(100)
            .controlResponseChannel(memberSpecificPort(archiveControlResponseChannel(index), index))
            .controlResponseStreamId(110 + index)
            .aeronDirectoryName(baseDirName)
            .errorHandler(th ->
            {
                System.err.println("[aeronArchiveContext-" + index + "]");
                th.printStackTrace(System.err);
            });

        context.mediaDriverContext
            .nameResolver(nameResolver)
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .errorHandler(ClusterTests.errorHandler(index))
            .dirDeleteOnShutdown(false)
            .dirDeleteOnStart(true)
            .errorHandler(th ->
            {
                System.err.println("[mediaDriverContext-" + index + "]");
                th.printStackTrace(System.err);
            });

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
            .deleteArchiveOnStart(true)
            .errorHandler(th ->
            {
                System.err.println("[archiveContext-" + index + "]");
                th.printStackTrace(System.err);
            });

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
            .deleteDirOnStart(true)
            .errorHandler(th ->
            {
                System.err.println("[consensusModuleContext-" + index + "]");
                th.printStackTrace(System.err);
            });

        context.serviceContainerContext
            .aeronDirectoryName(aeronDirName)
            .archiveContext(context.aeronArchiveContext.clone()
            .controlRequestChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL)
            .controlResponseChannel(ARCHIVE_LOCAL_CONTROL_CHANNEL))
            .clusterDir(new File(baseDirName, "service"))
            .clusteredService(context.service)
            .errorHandler(th ->
            {
                System.err.println("[serviceContainerContext-" + index + "]");
                th.printStackTrace(System.err);
            });

        nodes[index] = new TestNode(context, dataCollector);

        return nodes[index];
    }

    void stopNode(final TestNode testNode)
    {
        testNode.close();
    }

    void stopAllNodes()
    {
        CloseHelper.closeAll(nodes);
    }

    AeronCluster connectClient()
    {
        return connectClient(
            new AeronCluster.Context()
                .ingressChannel(INGRESS_CHANNEL)
                .egressChannel(egressChannel(0)));
    }

    AeronCluster connectClient(final AeronCluster.Context clientCtx)
    {
        final String aeronDirName = CommonContext.getAeronDirectoryName();

        if (null == clientMediaDriver)
        {
            dataCollector.add(Paths.get(aeronDirName));

            clientMediaDriver = MediaDriver.launch(
                new MediaDriver.Context()
                    .nameResolver(nameResolver)
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(false)
                    .aeronDirectoryName(aeronDirName)
                    .errorHandler(th ->
                    {
                        System.err.println("[clientMediaDriver-]");
                        th.printStackTrace(System.err);
                    }));
        }

        CloseHelper.close(client);
        client = AeronCluster.connect(
            clientCtx
                .aeronDirectoryName(aeronDirName)
                .egressListener(egressMessageListener)
                .ingressEndpoints(staticClusterMemberEndpoints)
                .errorHandler(th ->
                {
                    System.err.println("[client-]");
                    th.printStackTrace(System.err);
                }));

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
            try
            {
                pollUntilMessageSent(BitUtil.SIZE_OF_INT);
            }
            catch (final Throwable ex)
            {
                throw new ClusterException("failed to send message " + i + " of " + messageCount,
                    ex);
            }
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
                .append("local-").append(i).append(":2").append(clusterId).append("11").append(i)
                .append(',')
                .append("local-").append(i).append(":2").append(clusterId).append("22").append(i)
                .append(',')
                .append("local-").append(i).append(":2").append(clusterId).append("33").append(i)
                .append(',')
                .append("local-").append(i).append(":2").append(clusterId).append("44").append(i)
                .append(',')
                .append("local-").append(i).append(":801").append(i).append('|');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    static String ingressEndpoints(final int clusterId, final int memberCount)
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < memberCount; i++)
        {
            builder.append(i).append('=').append("local-").append(i).append(":2").append(clusterId)
                .append("11").append(i).append(',');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    private static String archiveControlRequestChannel(final int index)
    {
        return "aeron:udp?term-length=64k|endpoint=local-" + index + ":8010";
    }

    private static String archiveControlResponseChannel(final int index)
    {
        return "aeron:udp?term-length=64k|endpoint=local-" + index + ":8020";
    }

    private static String egressChannel(final int index)
    {
        return "aeron:udp?term-length=128k|endpoint=local-" + index + ":9020";
    }

    public void dumpData(final TestInfo testInfo)
    {
        dataCollector.dumpData(testInfo);
    }
}

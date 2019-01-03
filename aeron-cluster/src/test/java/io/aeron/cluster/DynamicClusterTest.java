/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.aeron.Aeron.NULL_VALUE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@Ignore
public class DynamicClusterTest
{
    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final int MAX_MEMBER_COUNT = 4;
    private static final int STATIC_MEMBER_COUNT = 3;
    private static final int MESSAGE_COUNT = 10;
    private static final String MSG = "Hello World!";

    private static final String CLUSTER_MEMBERS = clusterMembersString();
    private static final String[] CLUSTER_MEMBERS_ENDPOINTS = clusterMembersEndpoints();
    private static final String CLUSTER_MEMBERS_STATUS_ENDPOINTS = clusterMembersStatusEndpoints();
    private static final String LOG_CHANNEL =
        "aeron:udp?term-length=64k|control-mode=manual|control=localhost:55550";
    private static final String ARCHIVE_CONTROL_REQUEST_CHANNEL =
        "aeron:udp?term-length=64k|endpoint=localhost:8010";
    private static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL =
        "aeron:udp?term-length=64k|endpoint=localhost:8020";

    private final AtomicLong timeOffset = new AtomicLong();
    private final EpochClock epochClock = () -> System.currentTimeMillis() + timeOffset.get();

    private final CountDownLatch latchOne = new CountDownLatch(MAX_MEMBER_COUNT);
    private final CountDownLatch latchTwo = new CountDownLatch(MAX_MEMBER_COUNT - 1);

    private final EchoService[] echoServices = new EchoService[MAX_MEMBER_COUNT];
    private final AtomicBoolean[] terminationExpected = new AtomicBoolean[MAX_MEMBER_COUNT];
    private final AtomicBoolean[] serviceWasTerminated = new AtomicBoolean[MAX_MEMBER_COUNT];
    private final AtomicBoolean[] memberWasTerminated = new AtomicBoolean[MAX_MEMBER_COUNT];
    private final ClusteredMediaDriver[] clusteredMediaDrivers = new ClusteredMediaDriver[MAX_MEMBER_COUNT];
    private final ClusteredServiceContainer[] containers = new ClusteredServiceContainer[MAX_MEMBER_COUNT];
    private MediaDriver clientMediaDriver;
    private AeronCluster client;

    private final MutableInteger responseCount = new MutableInteger();
    private final EgressListener egressMessageListener =
        (clusterSessionId, timestamp, buffer, offset, length, header) -> responseCount.value++;

    @After
    public void after()
    {
        CloseHelper.close(client);
        CloseHelper.close(clientMediaDriver);

        if (null != clientMediaDriver)
        {
            clientMediaDriver.context().deleteAeronDirectory();
        }

        for (final ClusteredServiceContainer container : containers)
        {
            CloseHelper.close(container);
        }

        for (final ClusteredMediaDriver driver : clusteredMediaDrivers)
        {
            CloseHelper.close(driver);

            if (null != driver)
            {
                driver.mediaDriver().context().deleteAeronDirectory();
                driver.consensusModule().context().deleteDirectory();
                driver.archive().context().deleteArchiveDirectory();
            }
        }
    }

    @Test(timeout = 10_000)
    public void shouldQueryClusterMembers() throws Exception
    {
        for (int i = 0; i < STATIC_MEMBER_COUNT; i++)
        {
            startStaticNode(i, true);
        }

        int leaderMemberId;
        while (NULL_VALUE == (leaderMemberId = findLeaderId(NULL_VALUE)))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        final ClusterTool.ClusterMembersInfo clusterMembersInfo = new ClusterTool.ClusterMembersInfo();

        assertTrue(ClusterTool.listMembers(
            clusterMembersInfo, consensusModuleDir(leaderMemberId), TimeUnit.SECONDS.toMillis(1)));

        assertThat(clusterMembersInfo.leaderMemberId, is(leaderMemberId));
        assertThat(clusterMembersInfo.passiveMembers, is(""));
        assertThat(clusterMembersInfo.activeMembers, is(CLUSTER_MEMBERS));
    }

    @Test(timeout = 10_000)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshots() throws Exception
    {
        for (int i = 0; i < STATIC_MEMBER_COUNT; i++)
        {
            startStaticNode(i, true);
        }

        int leaderMemberId;
        while (NULL_VALUE == (leaderMemberId = findLeaderId(NULL_VALUE)))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        final int dynamicMemberIndex = STATIC_MEMBER_COUNT;
        startDynamicNode(dynamicMemberIndex, true);

        Thread.sleep(1000);

        assertThat(roleOf(dynamicMemberIndex), is(Cluster.Role.FOLLOWER));

        final ClusterTool.ClusterMembersInfo clusterMembersInfo = new ClusterTool.ClusterMembersInfo();

        assertTrue(ClusterTool.listMembers(
            clusterMembersInfo, consensusModuleDir(leaderMemberId), TimeUnit.SECONDS.toMillis(1)));

        assertThat(clusterMembersInfo.leaderMemberId, is(leaderMemberId));
        assertThat(clusterMembersInfo.passiveMembers, is(""));
    }

    @Test(timeout = 10_000)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsThenSend() throws Exception
    {
        for (int i = 0; i < STATIC_MEMBER_COUNT; i++)
        {
            startStaticNode(i, true);
        }

        while (NULL_VALUE == findLeaderId(NULL_VALUE))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        final int dynamicMemberIndex = STATIC_MEMBER_COUNT;
        startDynamicNode(dynamicMemberIndex, true);

        Thread.sleep(1000);

        assertThat(roleOf(dynamicMemberIndex), is(Cluster.Role.FOLLOWER));

        startClient();

        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        msgBuffer.putStringWithoutLengthAscii(0, MSG);

        sendMessages(msgBuffer);
        awaitResponses(MESSAGE_COUNT);
        awaitMessageCountForService(dynamicMemberIndex, MESSAGE_COUNT);
    }

    @Test(timeout = 10_000)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsWithCatchup() throws Exception
    {
        for (int i = 0; i < STATIC_MEMBER_COUNT; i++)
        {
            startStaticNode(i, true);
        }

        while (NULL_VALUE == findLeaderId(NULL_VALUE))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        startClient();

        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        msgBuffer.putStringWithoutLengthAscii(0, MSG);

        sendMessages(msgBuffer);
        awaitResponses(MESSAGE_COUNT);

        final int dynamicMemberIndex = STATIC_MEMBER_COUNT;
        startDynamicNode(dynamicMemberIndex, true);

        awaitMessageCountForService(dynamicMemberIndex, MESSAGE_COUNT);
    }

    @Test(timeout = 10_000)
    public void shouldDynamicallyJoinClusterOfThreeWithEmptySnapshot() throws Exception
    {
        for (int i = 0; i < STATIC_MEMBER_COUNT; i++)
        {
            startStaticNode(i, true);
        }

        int leaderMemberId;
        while (NULL_VALUE == (leaderMemberId = findLeaderId(NULL_VALUE)))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        takeSnapshot(leaderMemberId);
        awaitsSnapshotCounter(0, 1);
        awaitsSnapshotCounter(1, 1);
        awaitsSnapshotCounter(2, 1);

        final int dynamicMemberIndex = STATIC_MEMBER_COUNT;
        startDynamicNode(dynamicMemberIndex, true);

        Thread.sleep(1000);

        assertThat(roleOf(dynamicMemberIndex), is(Cluster.Role.FOLLOWER));

        awaitSnapshotLoadedForService(dynamicMemberIndex);
    }

    @Test(timeout = 10_000)
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshot() throws Exception
    {
        for (int i = 0; i < STATIC_MEMBER_COUNT; i++)
        {
            startStaticNode(i, true);
        }

        int leaderMemberId;
        while (NULL_VALUE == (leaderMemberId = findLeaderId(NULL_VALUE)))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        startClient();

        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        msgBuffer.putStringWithoutLengthAscii(0, MSG);

        sendMessages(msgBuffer);
        awaitResponses(MESSAGE_COUNT);

        takeSnapshot(leaderMemberId);
        awaitsSnapshotCounter(0, 1);
        awaitsSnapshotCounter(1, 1);
        awaitsSnapshotCounter(2, 1);

        final int dynamicMemberIndex = STATIC_MEMBER_COUNT;
        startDynamicNode(dynamicMemberIndex, true);

        Thread.sleep(1000);

        assertThat(roleOf(dynamicMemberIndex), is(Cluster.Role.FOLLOWER));

        awaitSnapshotLoadedForService(dynamicMemberIndex);
        awaitMessageCountForService(dynamicMemberIndex, MESSAGE_COUNT);
        assertThat(echoServices[dynamicMemberIndex].messageCount(), is(MESSAGE_COUNT));
    }

    @Test(timeout = 10_000)
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshotThenSend() throws Exception
    {
        for (int i = 0; i < STATIC_MEMBER_COUNT; i++)
        {
            startStaticNode(i, true);
        }

        int leaderMemberId;
        while (NULL_VALUE == (leaderMemberId = findLeaderId(NULL_VALUE)))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        startClient();

        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        msgBuffer.putStringWithoutLengthAscii(0, MSG);

        sendMessages(msgBuffer);
        awaitResponses(MESSAGE_COUNT);

        takeSnapshot(leaderMemberId);
        awaitsSnapshotCounter(0, 1);
        awaitsSnapshotCounter(1, 1);
        awaitsSnapshotCounter(2, 1);

        final int dynamicMemberIndex = STATIC_MEMBER_COUNT;
        startDynamicNode(dynamicMemberIndex, true);

        Thread.sleep(1000);

        assertThat(roleOf(dynamicMemberIndex), is(Cluster.Role.FOLLOWER));

        awaitSnapshotLoadedForService(dynamicMemberIndex);
        awaitMessageCountForService(dynamicMemberIndex, MESSAGE_COUNT);
        assertThat(echoServices[dynamicMemberIndex].messageCount(), is(MESSAGE_COUNT));

        sendMessages(msgBuffer);
        awaitResponses(MESSAGE_COUNT * 2);
        awaitMessageCountForService(dynamicMemberIndex, MESSAGE_COUNT * 2);
    }

    @Test(timeout = 10_000)
    public void shouldRemoveFollower() throws Exception
    {
        for (int i = 0; i < STATIC_MEMBER_COUNT; i++)
        {
            startStaticNode(i, true);
        }

        int leaderMemberId;
        while (NULL_VALUE == (leaderMemberId = findLeaderId(NULL_VALUE)))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        final int followerMemberId = (leaderMemberId + 1) >= STATIC_MEMBER_COUNT ? 0 : (leaderMemberId + 1);

        terminationExpected[followerMemberId].lazySet(true);

        assertTrue(ClusterTool.removeMember(consensusModuleDir(leaderMemberId), followerMemberId, false));

        while (!memberWasTerminated[followerMemberId].get())
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1);
        }

        stopNode(followerMemberId);

        final ClusterTool.ClusterMembersInfo clusterMembersInfo = new ClusterTool.ClusterMembersInfo();

        assertTrue(ClusterTool.listMembers(
            clusterMembersInfo, consensusModuleDir(leaderMemberId), TimeUnit.SECONDS.toMillis(1)));

        assertThat(numberOfMembers(clusterMembersInfo), is(STATIC_MEMBER_COUNT - 1));
        assertThat(clusterMembersInfo.leaderMemberId, is(leaderMemberId));
    }

    @Test(timeout = 10_000)
    public void shouldRemoveLeader() throws Exception
    {
        for (int i = 0; i < STATIC_MEMBER_COUNT; i++)
        {
            startStaticNode(i, true);
        }

        int leaderMemberId;
        while (NULL_VALUE == (leaderMemberId = findLeaderId(NULL_VALUE)))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        terminationExpected[leaderMemberId].lazySet(true);

        assertTrue(ClusterTool.removeMember(consensusModuleDir(leaderMemberId), leaderMemberId, false));

        while (!memberWasTerminated[leaderMemberId].get())
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1);
        }

        stopNode(leaderMemberId);

        while (NULL_VALUE == (leaderMemberId = findLeaderId(leaderMemberId)))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        final ClusterTool.ClusterMembersInfo clusterMembersInfo = new ClusterTool.ClusterMembersInfo();

        assertTrue(ClusterTool.listMembers(
            clusterMembersInfo, consensusModuleDir(leaderMemberId), TimeUnit.SECONDS.toMillis(1)));

        assertThat(numberOfMembers(clusterMembersInfo), is(STATIC_MEMBER_COUNT - 1));
        assertThat(clusterMembersInfo.leaderMemberId, is(leaderMemberId));
    }

    private void startStaticNode(final int index, final boolean cleanStart)
    {
        echoServices[index] = new EchoService(index, latchOne, latchTwo);
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";

        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, index))
            .controlRequestStreamId(100)
            .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, index))
            .controlResponseStreamId(110 + index)
            .aeronDirectoryName(baseDirName);

        terminationExpected[index] = new AtomicBoolean(false);
        memberWasTerminated[index] = new AtomicBoolean(false);
        serviceWasTerminated[index] = new AtomicBoolean(false);

        clusteredMediaDrivers[index] = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirName)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                .errorHandler(Throwable::printStackTrace)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .aeronDirectoryName(aeronDirName)
                .archiveDir(new File(baseDirName, "archive"))
                .controlChannel(archiveCtx.controlRequestChannel())
                .controlStreamId(archiveCtx.controlRequestStreamId())
                .localControlChannel("aeron:ipc?term-length=64k")
                .localControlStreamId(archiveCtx.controlRequestStreamId())
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(cleanStart),
            new ConsensusModule.Context()
                .epochClock(epochClock)
                .errorHandler(Throwable::printStackTrace)
                .clusterMemberId(index)
                .clusterMembers(CLUSTER_MEMBERS)
                .aeronDirectoryName(aeronDirName)
                .clusterDir(new File(baseDirName, "consensus-module"))
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel(memberSpecificPort(LOG_CHANNEL, index))
                .terminationHook(TestUtil.dynamicTerminationHook(
                    terminationExpected[index], memberWasTerminated[index]))
                .archiveContext(archiveCtx.clone())
                .deleteDirOnStart(cleanStart));

        containers[index] = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDirName)
                .archiveContext(archiveCtx.clone())
                .clusterDir(new File(baseDirName, "service"))
                .clusteredService(echoServices[index])
                .terminationHook(TestUtil.dynamicTerminationHook(
                    terminationExpected[index], serviceWasTerminated[index]))
                .errorHandler(Throwable::printStackTrace));
    }

    private void startDynamicNode(final int index, final boolean cleanStart)
    {
        echoServices[index] = new EchoService(index, latchOne, latchTwo);
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";

        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, index))
            .controlRequestStreamId(100)
            .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, index))
            .controlResponseStreamId(110 + index)
            .aeronDirectoryName(baseDirName);

        clusteredMediaDrivers[index] = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirName)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                .errorHandler(Throwable::printStackTrace)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .aeronDirectoryName(aeronDirName)
                .archiveDir(new File(baseDirName, "archive"))
                .controlChannel(archiveCtx.controlRequestChannel())
                .controlStreamId(archiveCtx.controlRequestStreamId())
                .localControlChannel("aeron:ipc?term-length=64k")
                .localControlStreamId(archiveCtx.controlRequestStreamId())
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(cleanStart),
            new ConsensusModule.Context()
                .epochClock(epochClock)
                .errorHandler(Throwable::printStackTrace)
                .clusterMemberId(NULL_VALUE)
                .clusterMembers("")
                .clusterMembersStatusEndpoints(CLUSTER_MEMBERS_STATUS_ENDPOINTS)
                .memberEndpoints(CLUSTER_MEMBERS_ENDPOINTS[index])
                .aeronDirectoryName(aeronDirName)
                .clusterDir(new File(baseDirName, "consensus-module"))
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel(memberSpecificPort(LOG_CHANNEL, index))
                .terminationHook(TestUtil.TERMINATION_HOOK)
                .archiveContext(archiveCtx.clone())
                .deleteDirOnStart(cleanStart));

        containers[index] = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDirName)
                .archiveContext(archiveCtx.clone())
                .clusterDir(new File(baseDirName, "service"))
                .clusteredService(echoServices[index])
                .terminationHook(TestUtil.TERMINATION_HOOK)
                .errorHandler(Throwable::printStackTrace));
    }

    private void stopNode(final int index)
    {
        containers[index].close();
        containers[index] = null;
        clusteredMediaDrivers[index].close();
        clusteredMediaDrivers[index] = null;
    }

    private void startClient()
    {
        final String aeronDirName = CommonContext.getAeronDirectoryName();

        clientMediaDriver = MediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .aeronDirectoryName(aeronDirName));

        client = AeronCluster.connect(
            new AeronCluster.Context()
                .egressListener(egressMessageListener)
                .aeronDirectoryName(aeronDirName)
                .ingressChannel("aeron:udp")
                .clusterMemberEndpoints("0=localhost:20110,1=localhost:20111,2=localhost:20112"));
    }

    private File consensusModuleDir(final int index)
    {
        return clusteredMediaDrivers[index].consensusModule().context().clusterDir();
    }

    private void sendMessages(final ExpandableArrayBuffer msgBuffer)
    {
        for (int i = 0; i < MESSAGE_COUNT; i++)
        {
            while (client.offer(msgBuffer, 0, MSG.length()) < 0)
            {
                TestUtil.checkInterruptedStatus();
                client.pollEgress();
                Thread.yield();
            }

            client.pollEgress();
        }
    }

    private void awaitResponses(final int messageCount)
    {
        while (responseCount.get() < messageCount)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
            client.pollEgress();
        }
    }

    private void awaitMessageCountForService(final int index, final int messageCount)
    {
        while (echoServices[index].messageCount() < messageCount)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }
    }

    private void awaitSnapshotLoadedForService(final int index)
    {
        while (!echoServices[index].wasSnapshotLoaded())
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }
    }

    private static String memberSpecificPort(final String channel, final int memberId)
    {
        return channel.substring(0, channel.length() - 1) + memberId;
    }

    private static String clusterMembersString()
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < STATIC_MEMBER_COUNT; i++)
        {
            builder
                .append(i).append(',')
                .append("localhost:2011").append(i).append(',')
                .append("localhost:2022").append(i).append(',')
                .append("localhost:2033").append(i).append(',')
                .append("localhost:2044").append(i).append(',')
                .append("localhost:801").append(i).append('|');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    private static String[] clusterMembersEndpoints()
    {
        final String[] clusterMembersEndpoints = new String[MAX_MEMBER_COUNT];

        for (int i = 0; i < MAX_MEMBER_COUNT; i++)
        {
            clusterMembersEndpoints[i] = "localhost:2011" + i + ',' +
                "localhost:2022" + i + ',' +
                "localhost:2033" + i + ',' +
                "localhost:2044" + i + ',' +
                "localhost:801" + i;
        }

        return clusterMembersEndpoints;
    }

    private static String clusterMembersStatusEndpoints()
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < STATIC_MEMBER_COUNT; i++)
        {
            builder.append("localhost:2022").append(i).append(',');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    static class EchoService extends StubClusteredService
    {
        private volatile int messageCount = 0;
        private volatile boolean wasSnapshotTaken = false;
        private volatile boolean wasSnapshotLoaded = false;
        private final int index;
        private final AtomicReference<String> messageCountString = new AtomicReference<>("no snapshot loaded");
        private final CountDownLatch latchOne;
        private final CountDownLatch latchTwo;

        EchoService(final int index, final CountDownLatch latchOne, final CountDownLatch latchTwo)
        {
            this.index = index;
            this.latchOne = latchOne;
            this.latchTwo = latchTwo;
        }

        int index()
        {
            return index;
        }

        int messageCount()
        {
            return messageCount;
        }

        String messageCountString()
        {
            return messageCountString.get();
        }

        boolean wasSnapshotTaken()
        {
            return wasSnapshotTaken;
        }

        boolean wasSnapshotLoaded()
        {
            return wasSnapshotLoaded;
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestampMs,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            while (session.offer(buffer, offset, length) < 0)
            {
                cluster.idle();
            }

            ++messageCount;
            messageCountString.set(Integer.toString(messageCount));

            if (messageCount == MESSAGE_COUNT)
            {
                latchOne.countDown();
            }

            if (messageCount == (MESSAGE_COUNT * 2))
            {
                latchTwo.countDown();
            }
        }

        public void onTakeSnapshot(final Publication snapshotPublication)
        {
            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

            int length = 0;
            buffer.putInt(length, messageCount);
            length += SIZE_OF_INT;
            length += buffer.putIntAscii(length, messageCount);

            snapshotPublication.offer(buffer, 0, length);
            wasSnapshotTaken = true;
        }

        public void onLoadSnapshot(final Image snapshotImage)
        {
            while (true)
            {
                final int fragments = snapshotImage.poll(
                    (buffer, offset, length, header) ->
                    {
                        messageCount = buffer.getInt(offset);

                        final String s = buffer.getStringWithoutLengthAscii(
                            offset + SIZE_OF_INT, length - SIZE_OF_INT);

                        messageCountString.set(s);
                    },
                    1);

                if (fragments == 1)
                {
                    break;
                }

                cluster.idle();
            }

            wasSnapshotLoaded = true;
        }
    }

    private int findLeaderId(final int skipMemberId)
    {
        int leaderMemberId = NULL_VALUE;

        for (int i = 0; i < 3; i++)
        {
            if (i == skipMemberId)
            {
                continue;
            }

            final ClusteredMediaDriver driver = clusteredMediaDrivers[i];

            if (null == driver)
            {
                continue;
            }

            final Cluster.Role role = Cluster.Role.get(
                (int)driver.consensusModule().context().clusterNodeCounter().get());

            if (Cluster.Role.LEADER == role)
            {
                leaderMemberId = driver.consensusModule().context().clusterMemberId();
            }
        }

        return leaderMemberId;
    }

    private Cluster.Role roleOf(final int index)
    {
        final ClusteredMediaDriver driver = clusteredMediaDrivers[index];

        return Cluster.Role.get((int)driver.consensusModule().context().clusterNodeCounter().get());
    }

    private void takeSnapshot(final int index)
    {
        final ClusteredMediaDriver driver = clusteredMediaDrivers[index];

        final CountersReader countersReader = driver.consensusModule().context().aeron().countersReader();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(countersReader);
        assertNotNull(controlToggle);
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));
    }

    private void awaitsSnapshotCounter(final int index, final long value)
    {
        final ClusteredMediaDriver driver = clusteredMediaDrivers[index];
        final Counter snapshotCounter = driver.consensusModule().context().snapshotCounter();

        while (snapshotCounter.getWeak() != value)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }
    }

    private int numberOfMembers(final ClusterTool.ClusterMembersInfo clusterMembersInfo)
    {
        final String[] members = clusterMembersInfo.activeMembers.split("\\|");

        return members.length;
    }
}

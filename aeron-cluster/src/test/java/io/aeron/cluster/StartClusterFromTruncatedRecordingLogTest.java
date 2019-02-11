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
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveMarkFile;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.RecordingLog.RECORDING_LOG_FILE_NAME;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@Ignore
public class StartClusterFromTruncatedRecordingLogTest
{
    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final int MEMBER_COUNT = 3;
    private static final int MESSAGE_COUNT = 10;
    private static final String MSG = "Hello World!";

    private static final String CLUSTER_MEMBERS = clusterMembersString();
    private static final String LOG_CHANNEL =
        "aeron:udp?term-length=256k|control-mode=manual|control=localhost:55550";
    private static final String ARCHIVE_CONTROL_REQUEST_CHANNEL =
        "aeron:udp?term-length=64k|endpoint=localhost:8010";
    private static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL =
        "aeron:udp?term-length=64k|endpoint=localhost:8020";

    private final AtomicLong timeOffset = new AtomicLong();
    private final EpochClock epochClock = () -> System.currentTimeMillis() + timeOffset.get();

    private final CountDownLatch latchOne = new CountDownLatch(MEMBER_COUNT);
    private final CountDownLatch latchTwo = new CountDownLatch(MEMBER_COUNT - 1);

    private final EchoService[] echoServices = new EchoService[MEMBER_COUNT];
    private final ClusteredMediaDriver[] clusteredMediaDrivers = new ClusteredMediaDriver[MEMBER_COUNT];
    private final ClusteredServiceContainer[] containers = new ClusteredServiceContainer[MEMBER_COUNT];
    private MediaDriver clientMediaDriver;
    private AeronCluster client;

    private final MutableInteger responseCount = new MutableInteger();
    private final EgressListener egressMessageListener =
        (clusterSessionId, timestamp, buffer, offset, length, header) -> responseCount.value++;
    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    @Before
    public void before()
    {
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            startNode(i, true);
        }
    }

    @After
    public void after() throws InterruptedException
    {
        executor.shutdownNow();

        CloseHelper.close(client);
        CloseHelper.close(clientMediaDriver);

        if (null != clientMediaDriver)
        {
            clientMediaDriver.context().deleteAeronDirectory();
        }

        for (final ClusteredServiceContainer container : containers)
        {
            CloseHelper.close(container);

            if (null != container)
            {
                container.context().deleteDirectory();
            }
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

        if (!executor.awaitTermination(5, TimeUnit.SECONDS))
        {
            System.out.println("Warning: not all tasks completed promptly");
        }
    }

    @Test(timeout = 45_000)
    public void shouldBeAbleToStartClusterFromTruncatedRecordingLog() throws Exception
    {
        stopAndStartClusterWithTruncationOfRecordingLog();
        assertClusterIsFunctioningCorrectly();
        stopAndStartClusterWithTruncationOfRecordingLog();
        assertClusterIsFunctioningCorrectly();
        stopAndStartClusterWithTruncationOfRecordingLog();
        assertClusterIsFunctioningCorrectly();
        stopAndStartClusterWithTruncationOfRecordingLog();
        assertClusterIsFunctioningCorrectly();
    }

    private void stopAndStartClusterWithTruncationOfRecordingLog() throws InterruptedException, IOException
    {
        int leaderMemberId;
        while (NULL_VALUE == (leaderMemberId = findLeaderId(NULL_VALUE)))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        final int followerMemberIdA = (leaderMemberId + 1) >= MEMBER_COUNT ? 0 : (leaderMemberId + 1);
        final int followerMemberIdB = (followerMemberIdA + 1) >= MEMBER_COUNT ? 0 : (followerMemberIdA + 1);

        takeSnapshot(leaderMemberId);
        awaitSnapshotCounter(leaderMemberId, 1);
        awaitSnapshotCounter(followerMemberIdA, 1);
        awaitSnapshotCounter(followerMemberIdB, 1);

        awaitNeutralCounter(leaderMemberId);
        awaitNeutralCounter(followerMemberIdA);
        awaitNeutralCounter(followerMemberIdB);

        shutdown(leaderMemberId);
        awaitSnapshotCounter(leaderMemberId, 2);
        awaitSnapshotCounter(followerMemberIdA, 2);
        awaitSnapshotCounter(followerMemberIdB, 2);

        stopNode(leaderMemberId);
        stopNode(followerMemberIdA);
        stopNode(followerMemberIdB);

        truncateRecordingLogAndDeleteMarkFiles(leaderMemberId);
        truncateRecordingLogAndDeleteMarkFiles(followerMemberIdA);
        truncateRecordingLogAndDeleteMarkFiles(followerMemberIdB);

        Thread.sleep(1000);

        startNode(leaderMemberId, false);
        startNode(followerMemberIdA, false);
        startNode(followerMemberIdB, false);
    }

    private void assertClusterIsFunctioningCorrectly() throws InterruptedException
    {
        int leaderMemberId;
        while (NULL_VALUE == (leaderMemberId = findLeaderId(NULL_VALUE)))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        final int followerMemberIdA = (leaderMemberId + 1) >= MEMBER_COUNT ? 0 : (leaderMemberId + 1);
        final int followerMemberIdB = (leaderMemberId + 1) >= MEMBER_COUNT ? 0 : (leaderMemberId + 1);

        Thread.sleep(1000);

        assertThat(roleOf(followerMemberIdA), is(Cluster.Role.FOLLOWER));
        assertThat(roleOf(followerMemberIdB), is(Cluster.Role.FOLLOWER));

        startClient();

        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        msgBuffer.putStringWithoutLengthAscii(0, MSG);

        final int initialCount = responseCount.get();
        sendMessages(msgBuffer);
        awaitResponses(MESSAGE_COUNT + initialCount);
    }

    private void truncateRecordingLogAndDeleteMarkFiles(final int index) throws IOException
    {
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;

        final File consensusModuleDataDir = new File(baseDirName, "consensus-module");
        final File archiveDataDir = new File(baseDirName, "archive");
        final File tmpRecordingFile = new File(baseDirName, RECORDING_LOG_FILE_NAME);
        deleteFile(tmpRecordingFile);
        deleteFile(new File(archiveDataDir, ArchiveMarkFile.FILENAME));
        deleteFile(new File(consensusModuleDataDir, ClusterMarkFile.FILENAME));

        try (RecordingLog existingRecordingLog = new RecordingLog(consensusModuleDataDir))
        {
            try (RecordingLog newRecordingLog = new RecordingLog(new File(baseDirName)))
            {
                final RecordingLog.Entry latestTermEntry = existingRecordingLog.entries().stream()
                    .filter(e -> e.type == RecordingLog.ENTRY_TYPE_TERM)
                    .max(Comparator.comparingLong(e -> e.logPosition))
                    .orElseThrow(() -> new IllegalStateException("No term entry in recording log"));

                newRecordingLog.appendTerm(latestTermEntry.recordingId, latestTermEntry.leadershipTermId,
                    latestTermEntry.termBaseLogPosition, latestTermEntry.timestamp);
                newRecordingLog.commitLogPosition(latestTermEntry.leadershipTermId, latestTermEntry.logPosition);

                appendServiceSnapshot(existingRecordingLog, newRecordingLog, -1);
                appendServiceSnapshot(existingRecordingLog, newRecordingLog, 0);
            }
        }

        Files.copy(new File(baseDirName).toPath().resolve(RECORDING_LOG_FILE_NAME),
            consensusModuleDataDir.toPath().resolve(RECORDING_LOG_FILE_NAME),
            StandardCopyOption.REPLACE_EXISTING);

        try (RecordingLog copiedRecordingLog = new RecordingLog(consensusModuleDataDir))
        {
            final LongHashSet recordingIds = new LongHashSet();
            copiedRecordingLog.entries().stream().mapToLong(e -> e.recordingId).forEach(recordingIds::add);
            try (Stream<Path> segments = Files.list(archiveDataDir.toPath())
                .filter((p) -> p.getFileName().toString().endsWith(".rec")))
            {
                segments.filter(
                    (p) ->
                    {
                        final String fileName = p.getFileName().toString();
                        final long recording = Long.parseLong(fileName.split("-")[0]);

                        return !recordingIds.contains(recording);
                    }).map(Path::toFile).forEach(this::deleteFile);
            }

            // assert that recording log is not growing
            assertTrue(copiedRecordingLog.entries().size() <= 3);
        }
    }

    private void deleteFile(final File file)
    {
        if (file.exists())
        {
            try
            {
                Files.delete(file.toPath());
            }
            catch (final IOException e)
            {
                Assert.fail("Failed to delete file: " + file);
            }
        }

        if (file.exists())
        {
            Assert.fail("Failed to delete file: " + file);
        }
    }

    private void appendServiceSnapshot(
        final RecordingLog existingRecordingLog, final RecordingLog newRecordingLog, final int serviceId)
    {
        final RecordingLog.Entry snapshot = existingRecordingLog.getLatestSnapshot(serviceId);
        newRecordingLog.appendSnapshot(
            snapshot.recordingId,
            snapshot.leadershipTermId,
            snapshot.termBaseLogPosition,
            snapshot.logPosition,
            snapshot.timestamp,
            snapshot.serviceId);
    }

    private void startNode(final int index, final boolean cleanStart)
    {
        if (null == echoServices[index])
        {
            echoServices[index] = new EchoService(index, latchOne, latchTwo);
        }
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";

        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, index))
            .controlRequestStreamId(100 + index)
            .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, index))
            .controlResponseStreamId(110 + index)
            .aeronDirectoryName(baseDirName);

        clusteredMediaDrivers[index] = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirName)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                .errorHandler(TestUtil.errorHandler(0))
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
                .errorHandler(Throwable::printStackTrace)
                .deleteArchiveOnStart(cleanStart),
            new ConsensusModule.Context()
                .epochClock(epochClock)
                .errorHandler(TestUtil.errorHandler(0))
                .clusterMemberId(index)
                .clusterMembers(CLUSTER_MEMBERS)
                .aeronDirectoryName(aeronDirName)
                .clusterDir(new File(baseDirName, "consensus-module"))
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel(memberSpecificPort(LOG_CHANNEL, index))
                .archiveContext(archiveCtx.clone())
                .deleteDirOnStart(cleanStart));

        containers[index] = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDirName)
                .archiveContext(archiveCtx.clone())
                .clusterDir(new File(baseDirName, "service"))
                .clusteredService(echoServices[index])
                .errorHandler(TestUtil.errorHandler(0)));
    }

    private void stopNode(final int index)
    {
        containers[index].close();
        containers[index] = null;
        clusteredMediaDrivers[index].close();
        clusteredMediaDrivers[index].mediaDriver().context().deleteAeronDirectory();
        clusteredMediaDrivers[index] = null;
    }

    private void startClient()
    {
        if (client != null)
        {
            client.close();
            client = null;
            clientMediaDriver.close();
        }

        final String aeronDirName = CommonContext.getAeronDirectoryName();

        clientMediaDriver = MediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .aeronDirectoryName(aeronDirName)
                .dirDeleteOnStart(true));

        client = AeronCluster.connect(
            new AeronCluster.Context()
                .egressListener(egressMessageListener)
                .aeronDirectoryName(aeronDirName)
                .ingressChannel("aeron:udp")
                .clusterMemberEndpoints("0=localhost:20110,1=localhost:20111,2=localhost:20112"));
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

        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            while (echoServices[i].messageCount() < messageCount)
            {
                TestUtil.checkInterruptedStatus();
                Thread.yield();
            }
        }
    }

    private static String memberSpecificPort(final String channel, final int memberId)
    {
        return channel.substring(0, channel.length() - 1) + memberId;
    }

    private static String clusterMembersString()
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < MEMBER_COUNT; i++)
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

    static class EchoService extends StubClusteredService
    {
        private volatile int messageCount;
        private final int index;
        private final CountDownLatch latchOne;
        private final CountDownLatch latchTwo;

        EchoService(final int index, final CountDownLatch latchOne, final CountDownLatch latchTwo)
        {
            this.index = index;
            this.latchOne = latchOne;
            this.latchTwo = latchTwo;
        }

        int messageCount()
        {
            return messageCount;
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

            if (messageCount == MESSAGE_COUNT)
            {
                latchOne.countDown();
            }

            if (messageCount == (MESSAGE_COUNT * 2))
            {
                latchTwo.countDown();
            }
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

    private void shutdown(final int index)
    {
        final AtomicCounter controlToggle = getControlToggle(index);
        assertNotNull(controlToggle);

        assertTrue(String.valueOf(ClusterControl.ToggleState.get(controlToggle)),
            ClusterControl.ToggleState.SHUTDOWN.toggle(controlToggle));
    }

    private AtomicCounter getControlToggle(final int index)
    {
        final ClusteredMediaDriver driver = clusteredMediaDrivers[index];
        final CountersReader countersReader = driver.consensusModule().context().aeron().countersReader();

        return ClusterControl.findControlToggle(countersReader);
    }

    private void awaitNeutralCounter(final int index)
    {
        final AtomicCounter controlToggle = getControlToggle(index);
        while (ClusterControl.ToggleState.get(controlToggle) != ClusterControl.ToggleState.NEUTRAL)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }
    }

    private void awaitSnapshotCounter(final int index, final long value)
    {
        final ClusteredMediaDriver driver = clusteredMediaDrivers[index];
        final Counter snapshotCounter = driver.consensusModule().context().snapshotCounter();

        while (snapshotCounter.get() != value)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }
    }
}

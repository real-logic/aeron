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

import io.aeron.*;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveMarkFile;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.*;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.test.SlowTest;
import io.aeron.test.Tests;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.StubClusteredService;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.RecordingLog.RECORDING_LOG_FILE_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SlowTest
public class StartFromTruncatedRecordingLogTest
{
    private static final long CATALOG_CAPACITY = 1024 * 1024;
    private static final int MEMBER_COUNT = 3;
    private static final int MESSAGE_COUNT = 10;

    private static final String CLUSTER_MEMBERS = clusterMembersString();
    private static final String LOG_CHANNEL = "aeron:udp?term-length=256k";
    private static final String ARCHIVE_CONTROL_REQUEST_CHANNEL = "aeron:ipc?term-length=64k";
    private static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL = "aeron:ipc?term-length=64k";

    private final AtomicLong[] snapshotCounters = new AtomicLong[MEMBER_COUNT];
    private final Counter[] mockSnapshotCounters = new Counter[MEMBER_COUNT];
    private final EchoService[] echoServices = new EchoService[MEMBER_COUNT];
    private final ClusteredMediaDriver[] clusteredMediaDrivers = new ClusteredMediaDriver[MEMBER_COUNT];
    private final ClusteredServiceContainer[] containers = new ClusteredServiceContainer[MEMBER_COUNT];
    private MediaDriver clientMediaDriver;
    private AeronCluster client;

    private final MutableInteger responseCount = new MutableInteger();
    private final EgressListener egressMessageListener =
        (clusterSessionId, timestamp, buffer, offset, length, header) -> responseCount.value++;
    private final AtomicLong terminateCount = new AtomicLong();

    @BeforeEach
    public void before()
    {
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            final AtomicLong atomicLong = new AtomicLong();
            final Counter mockCounter = mock(Counter.class);
            snapshotCounters[i] = atomicLong;
            mockSnapshotCounters[i] = mockCounter;
            when(mockCounter.incrementOrdered()).thenAnswer((invocation) -> atomicLong.getAndIncrement());

            echoServices[i] = new EchoService();
            startNode(i, true);
        }

        clientMediaDriver = MediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .warnIfDirectoryExists(false)
                .dirDeleteOnStart(true));
    }

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(client, clientMediaDriver);

        for (final ClusteredMediaDriver driver : clusteredMediaDrivers)
        {
            if (null != driver)
            {
                driver.consensusModule().close();
            }
        }

        CloseHelper.closeAll(containers);
        CloseHelper.closeAll(clusteredMediaDrivers);

        clientMediaDriver.context().deleteDirectory();

        for (final ClusteredServiceContainer container : containers)
        {
            if (null != container)
            {
                container.context().deleteDirectory();
            }
        }

        for (final ClusteredMediaDriver driver : clusteredMediaDrivers)
        {
            if (null != driver)
            {
                driver.consensusModule().context().deleteDirectory();
                driver.archive().context().deleteDirectory();
                driver.mediaDriver().context().deleteDirectory();
            }
        }
    }

    @Test
    @Timeout(30)
    public void shouldBeAbleToStartClusterFromTruncatedRecordingLog() throws Exception
    {
        stopAndStartClusterWithTruncationOfRecordingLog();
        assertClusterIsFunctioningCorrectly();

        stopAndStartClusterWithTruncationOfRecordingLog();
        assertClusterIsFunctioningCorrectly();

        stopAndStartClusterWithTruncationOfRecordingLog();
        assertClusterIsFunctioningCorrectly();

        ClusterTests.failOnClusterError();
    }

    private void stopAndStartClusterWithTruncationOfRecordingLog() throws Exception
    {
        final int leaderMemberId = awaitLeaderMemberId();
        final int followerMemberIdA = (leaderMemberId + 1) >= MEMBER_COUNT ? 0 : (leaderMemberId + 1);
        final int followerMemberIdB = (followerMemberIdA + 1) >= MEMBER_COUNT ? 0 : (followerMemberIdA + 1);

        takeSnapshot(leaderMemberId);
        awaitSnapshotCount(1);

        awaitNeutralControlToggle(leaderMemberId);
        awaitConsensusModulesActive();

        shutdown(leaderMemberId);
        awaitSnapshotCount(2);
        Tests.awaitValue(terminateCount, MEMBER_COUNT);

        stopNode(leaderMemberId);
        stopNode(followerMemberIdA);
        stopNode(followerMemberIdB);

        truncateRecordingLogAndDeleteMarkFiles(leaderMemberId);
        truncateRecordingLogAndDeleteMarkFiles(followerMemberIdA);
        truncateRecordingLogAndDeleteMarkFiles(followerMemberIdB);

        terminateCount.set(0);
        startNode(leaderMemberId, false);
        startNode(followerMemberIdA, false);
        startNode(followerMemberIdB, false);
    }

    private int awaitLeaderMemberId()
    {
        int leaderMemberId;
        while (NULL_VALUE == (leaderMemberId = findLeaderId()))
        {
            Tests.sleep(10);
        }

        return leaderMemberId;
    }

    private void assertClusterIsFunctioningCorrectly()
    {
        awaitLeaderMemberId();
        connectClient();

        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        msgBuffer.putStringWithoutLengthAscii(0, ClusterTests.HELLO_WORLD_MSG);

        final int initialCount = responseCount.get();
        sendMessages(msgBuffer);
        awaitResponses(MESSAGE_COUNT + initialCount);

        closeClient();
    }

    private void truncateRecordingLogAndDeleteMarkFiles(final int index) throws IOException
    {
        final String baseDirName = baseDirName(index);

        final File consensusModuleDataDir = new File(baseDirName, "consensus-module");
        final File archiveDataDir = new File(baseDirName, "archive");
        final File tmpRecordingFile = new File(baseDirName, RECORDING_LOG_FILE_NAME);
        deleteFile(tmpRecordingFile);
        deleteFile(new File(archiveDataDir, ArchiveMarkFile.FILENAME));
        deleteFile(new File(consensusModuleDataDir, ClusterMarkFile.FILENAME));

        try (RecordingLog recordingLog = new RecordingLog(consensusModuleDataDir))
        {
            final RecordingLog.Entry lastTermEntry = recordingLog.findLastTerm();
            if (null == lastTermEntry)
            {
                throw new IllegalStateException("no term found in recording log");
            }

            try (RecordingLog newRecordingLog = new RecordingLog(new File(baseDirName)))
            {
                newRecordingLog.appendTerm(
                    lastTermEntry.recordingId,
                    lastTermEntry.leadershipTermId,
                    lastTermEntry.termBaseLogPosition,
                    lastTermEntry.timestamp);
                newRecordingLog.commitLogPosition(lastTermEntry.leadershipTermId, lastTermEntry.logPosition);

                appendServiceSnapshot(recordingLog, newRecordingLog, 0);
                appendServiceSnapshot(recordingLog, newRecordingLog, ConsensusModule.Configuration.SERVICE_ID);
            }
        }

        Files.copy(
            new File(baseDirName).toPath().resolve(RECORDING_LOG_FILE_NAME),
            consensusModuleDataDir.toPath().resolve(RECORDING_LOG_FILE_NAME),
            StandardCopyOption.REPLACE_EXISTING);

        try (RecordingLog copiedRecordingLog = new RecordingLog(consensusModuleDataDir))
        {
            final LongHashSet recordingIds = new LongHashSet();
            copiedRecordingLog.entries().stream().mapToLong((e) -> e.recordingId).forEach(recordingIds::add);
            final Predicate<Path> filterPredicate = (p) -> p.getFileName().toString().endsWith(".rec");

            try (Stream<Path> segments = Files.list(archiveDataDir.toPath()).filter(filterPredicate))
            {
                segments.filter(
                    (p) ->
                    {
                        final String fileName = p.getFileName().toString();
                        final long recording = Long.parseLong(fileName.split("-")[0]);

                        return !recordingIds.contains(recording);
                    })
                    .map(Path::toFile).forEach(this::deleteFile);
            }

            assertTrue(copiedRecordingLog.entries().size() <= 3);
        }
    }

    private String baseDirName(final int index)
    {
        return CommonContext.getAeronDirectoryName() + "-" + index;
    }

    private String aeronDirName(final int index)
    {
        return CommonContext.getAeronDirectoryName() + "-" + index + "-driver";
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
                fail("failed to delete file: " + file);
            }
        }

        if (file.exists())
        {
            fail("failed to delete file: " + file);
        }
    }

    private void appendServiceSnapshot(
        final RecordingLog existingRecordingLog, final RecordingLog newRecordingLog, final int serviceId)
    {
        final RecordingLog.Entry snapshot = existingRecordingLog.getLatestSnapshot(serviceId);
        assertNotNull(snapshot);

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
        final String baseDirName = baseDirName(index);
        final String aeronDirName = aeronDirName(index);

        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(ARCHIVE_CONTROL_REQUEST_CHANNEL)
            .controlRequestStreamId(100 + index)
            .controlResponseChannel(ARCHIVE_CONTROL_RESPONSE_CHANNEL)
            .controlResponseStreamId(110 + index)
            .aeronDirectoryName(baseDirName);

        clusteredMediaDrivers[index] = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirName)
                .warnIfDirectoryExists(false)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .errorHandler(ClusterTests.errorHandler(index))
                .dirDeleteOnShutdown(false)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .catalogCapacity(CATALOG_CAPACITY)
                .archiveDir(new File(baseDirName, "archive"))
                .controlChannel(archiveCtx.controlRequestChannel())
                .controlStreamId(archiveCtx.controlRequestStreamId())
                .localControlChannel(ARCHIVE_CONTROL_RESPONSE_CHANNEL)
                .localControlStreamId(archiveCtx.controlRequestStreamId())
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .errorHandler(Tests::onError)
                .deleteArchiveOnStart(cleanStart),
            new ConsensusModule.Context()
                .errorHandler(ClusterTests.errorHandler(index))
                .terminationHook(terminateCount::incrementAndGet)
                .clusterMemberId(index)
                .snapshotCounter(mockSnapshotCounters[index])
                .clusterMembers(CLUSTER_MEMBERS)
                .clusterDir(new File(baseDirName, "consensus-module"))
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel(LOG_CHANNEL)
                .archiveContext(archiveCtx.clone())
                .deleteDirOnStart(cleanStart));

        containers[index] = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDirName)
                .archiveContext(archiveCtx.clone())
                .clusterDir(new File(baseDirName, "service"))
                .clusteredService(echoServices[index])
                .errorHandler(ClusterTests.errorHandler(index)));
    }

    private void stopNode(final int index)
    {
        containers[index].close();
        containers[index] = null;
        clusteredMediaDrivers[index].close();
        clusteredMediaDrivers[index] = null;
    }

    private void connectClient()
    {
        closeClient();

        client = AeronCluster.connect(
            new AeronCluster.Context()
                .egressListener(egressMessageListener)
                .ingressChannel("aeron:udp?term-length=64k")
                .egressChannel("aeron:udp?term-length=64k|endpoint=localhost:0")
                .ingressEndpoints("0=localhost:20110,1=localhost:20111,2=localhost:20112"));
    }

    private void closeClient()
    {
        if (null != client)
        {
            client.close();
            client = null;
        }
    }

    private void sendMessages(final ExpandableArrayBuffer msgBuffer)
    {
        for (int i = 0; i < MESSAGE_COUNT; i++)
        {
            while (client.offer(msgBuffer, 0, ClusterTests.HELLO_WORLD_MSG.length()) < 0)
            {
                Tests.yield();
                client.pollEgress();
            }

            client.pollEgress();
        }
    }

    private void awaitResponses(final int messageCount)
    {
        while (responseCount.get() < messageCount)
        {
            Thread.yield();
            if (Thread.currentThread().isInterrupted())
            {
                final String msg = "messageCount=" + messageCount + " responseCount=" + responseCount;
                Tests.unexpectedInterruptStackTrace(msg);
                fail("unexpected interrupt - " + msg);
            }

            client.pollEgress();
        }

        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            while (echoServices[i].messageCount() < messageCount)
            {
                Thread.yield();
                if (Thread.currentThread().isInterrupted())
                {
                    final String msg =
                        "memberId=" + i +
                        " messageCount=" + messageCount +
                        " serviceMessageCount=" + echoServices[i].messageCount();
                    Tests.unexpectedInterruptStackTrace(msg);
                    fail("unexpected interrupt - " + msg);
                }
            }
        }
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

        int messageCount()
        {
            return messageCount;
        }

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

            //noinspection NonAtomicOperationOnVolatileField
            ++messageCount;
        }
    }

    private int findLeaderId()
    {
        int leaderMemberId = NULL_VALUE;

        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            final ConsensusModule.Context context = clusteredMediaDrivers[i].consensusModule().context();
            final Cluster.Role role = Cluster.Role.get(context.clusterNodeRoleCounter());
            final Counter electionStateCounter = context.electionStateCounter();

            if (Cluster.Role.LEADER == role && ElectionState.CLOSED == ElectionState.get(electionStateCounter))
            {
                leaderMemberId = context.clusterMemberId();
            }
        }

        return leaderMemberId;
    }

    private void takeSnapshot(final int index)
    {
        final ConsensusModule.Context context = clusteredMediaDrivers[index].consensusModule().context();
        final CountersReader counters = context.aeron().countersReader();
        final int clusterId = context.clusterId();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters, clusterId);

        assertNotNull(controlToggle);
        awaitNeutralControlToggle(index);
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));
    }

    private void shutdown(final int index)
    {
        final AtomicCounter controlToggle = getControlToggle(index);
        assertNotNull(controlToggle);

        assertTrue(
            ClusterControl.ToggleState.SHUTDOWN.toggle(controlToggle),
            String.valueOf(ClusterControl.ToggleState.get(controlToggle)));
    }

    private AtomicCounter getControlToggle(final int index)
    {
        final ConsensusModule.Context context = clusteredMediaDrivers[index].consensusModule().context();
        if (context.aeron().isClosed())
        {
            return null;
        }

        return ClusterControl.findControlToggle(context.aeron().countersReader(), context.clusterId());
    }

    private void awaitNeutralControlToggle(final int index)
    {
        while (true)
        {
            final AtomicCounter controlToggle = getControlToggle(index);
            assertNotNull(controlToggle);

            if (ClusterControl.ToggleState.get(controlToggle) == ClusterControl.ToggleState.NEUTRAL)
            {
                break;
            }

            Tests.yield();
        }
    }

    private void awaitConsensusModulesActive()
    {
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            final ConsensusModule.Context context = clusteredMediaDrivers[i].consensusModule().context();
            final Counter moduleStateCounter = context.moduleStateCounter();

            while (ConsensusModule.State.ACTIVE != ConsensusModule.State.get(moduleStateCounter))
            {
                Tests.yield();
            }
        }
    }

    private void awaitSnapshotCount(final long count)
    {
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            while (snapshotCounters[i].get() < count)
            {
                Tests.sleep(1);
            }
        }
    }
}

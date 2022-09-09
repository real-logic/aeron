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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.ConsensusModuleDecoder;
import io.aeron.cluster.codecs.ConsensusModuleEncoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.NULL_SESSION_ID;
import static io.aeron.cluster.ConsensusModuleSnapshotPendingServiceMessagesPatch.replayLocalSnapshotRecording;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(InterruptingTestCallback.class)
class ConsensusModuleSnapshotPendingServiceMessagesPatchTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void setUp()
    {
        systemTestWatcher.ignoreErrorsMatching(
            (s) -> s.contains("ats_gcm_decrypt final_ex: error:00000000:lib(0):func(0):reason(0)"));
    }

    @AfterEach
    void tearDown()
    {
        TestNode.MessageTrackingService.delaySessionMessageProcessing(false);
    }

    @Test
    void executeThrowsNullPointerExceptionIfClusterDirIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> new ConsensusModuleSnapshotPendingServiceMessagesPatch().execute(null));
    }

    @Test
    void executeThrowsIllegalArgumentExceptionIfClusterDirDoesNotExist()
    {
        final File clusterDir = new File("non-existing-file-blah-blah");

        final IllegalArgumentException exception = assertThrowsExactly(
            IllegalArgumentException.class,
            () -> new ConsensusModuleSnapshotPendingServiceMessagesPatch().execute(clusterDir));
        assertEquals("invalid cluster directory: " + clusterDir.getAbsolutePath(), exception.getMessage());
    }

    @Test
    void executeThrowsIllegalArgumentExceptionIfClusterDirIsNotADirectory(
        final @TempDir File tempDir) throws IOException
    {
        final File clusterDir = new File(tempDir, "file.txt");
        assertTrue(clusterDir.createNewFile());

        final IllegalArgumentException exception = assertThrowsExactly(
            IllegalArgumentException.class,
            () -> new ConsensusModuleSnapshotPendingServiceMessagesPatch().execute(clusterDir));
        assertEquals("invalid cluster directory: " + clusterDir.getAbsolutePath(), exception.getMessage());
    }

    @Test
    void executeThrowsClusterExceptionIfClusterDirDoesNotContainARecordingLog(final @TempDir File tempDir)
    {
        final File clusterDir = new File(tempDir, "cluster-dir");
        assertTrue(clusterDir.mkdir());

        final ClusterException exception = assertThrowsExactly(
            ClusterException.class,
            () -> new ConsensusModuleSnapshotPendingServiceMessagesPatch().execute(clusterDir));
        final Throwable cause = exception.getCause();
        assertInstanceOf(IOException.class, cause);
    }

    @Test
    @SlowTest
    @InterruptAfter(30)
    void executeIsANoOpIfTheSnapshotIsInValidState()
    {
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withTimerServiceSupplier(new PriorityHeapTimerServiceSupplier())
            .withServiceSupplier((i) -> new TestNode.TestService[]{
                new TestNode.MessageTrackingService(1, i),
                new TestNode.MessageTrackingService(2, i) })
            .start();
        systemTestWatcher.cluster(cluster);
        final int serviceCount = cluster.node(0).services().length;

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();

        TestNode.MessageTrackingService.delaySessionMessageProcessing(true);
        int messageCount = 0;
        final ExpandableArrayBuffer msgBuffer = cluster.msgBuffer();
        for (int i = 0; i < 2222; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);
        TestNode.MessageTrackingService.delaySessionMessageProcessing(false);

        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitServiceMessages(cluster, serviceCount, messageCount);
        stopConsensusModulesAndServices(cluster);

        final File leaderClusterDir = leader.consensusModule().context().clusterDir();
        final RecordingLog.Entry leaderSnapshot = ClusterTool.findLatestValidSnapshot(leaderClusterDir);
        assertNotNull(leaderSnapshot);

        final MutableLong mutableNextSessionId = new MutableLong(NULL_SESSION_ID);
        final MutableLong mutableNextServiceSessionId = new MutableLong(NULL_SESSION_ID);
        final MutableLong mutableLogServiceSessionId = new MutableLong(NULL_SESSION_ID);
        final LongArrayList pendingMessageClusterSessionIds = new LongArrayList();
        final ConsensusModuleSnapshotListener stateReader = new NoOpConsensusModuleSnapshotListener()
        {
            public void onLoadConsensusModuleState(
                final long nextSessionId,
                final long nextServiceSessionId,
                final long logServiceSessionId,
                final int pendingMessageCapacity,
                final DirectBuffer buffer,
                final int offset,
                final int length)
            {
                mutableNextSessionId.set(nextSessionId);
                mutableNextServiceSessionId.set(nextServiceSessionId);
                mutableLogServiceSessionId.set(logServiceSessionId);
            }

            public void onLoadPendingMessage(
                final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
            {
                pendingMessageClusterSessionIds.add(clusterSessionId);
            }
        };

        readSnapshotRecording(leader, leaderSnapshot.recordingId, stateReader);

        final long beforeNextSessionId = mutableNextSessionId.get();
        final long beforeNextServiceSessionId = mutableNextServiceSessionId.get();
        final long beforeLogServiceSessionId = mutableLogServiceSessionId.get();
        final long[] beforeClusterSessionIds = pendingMessageClusterSessionIds.toLongArray();
        assertNotEquals(NULL_SESSION_ID, beforeNextSessionId);
        assertNotEquals(NULL_SESSION_ID, beforeNextServiceSessionId);
        assertNotEquals(NULL_SESSION_ID, beforeLogServiceSessionId);
        assertNotEquals(beforeNextSessionId, beforeNextServiceSessionId);
        assertNotEquals(beforeNextSessionId, beforeLogServiceSessionId);
        assertNotEquals(beforeNextServiceSessionId, beforeLogServiceSessionId);
        assertNotEquals(0, beforeClusterSessionIds.length);

        final ConsensusModuleSnapshotPendingServiceMessagesPatch snapshotPatch =
            new ConsensusModuleSnapshotPendingServiceMessagesPatch();
        assertFalse(snapshotPatch.execute(leaderClusterDir));

        mutableNextSessionId.set(NULL_SESSION_ID);
        mutableNextServiceSessionId.set(NULL_SESSION_ID);
        mutableLogServiceSessionId.set(NULL_SESSION_ID);
        pendingMessageClusterSessionIds.clear();
        readSnapshotRecording(leader, leaderSnapshot.recordingId, stateReader);
        assertEquals(beforeNextSessionId, mutableNextSessionId.get());
        assertEquals(beforeNextServiceSessionId, mutableNextServiceSessionId.get());
        assertEquals(beforeLogServiceSessionId, mutableLogServiceSessionId.get());
        assertArrayEquals(beforeClusterSessionIds, pendingMessageClusterSessionIds.toLongArray());
    }

    @Test
    @SlowTest
    @InterruptAfter(30)
    @SuppressWarnings("MethodLength")
    void executeShouldPatchTheStateOfTheLeaderSnapshot()
    {
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withTimerServiceSupplier(new PriorityHeapTimerServiceSupplier())
            .withServiceSupplier((i) -> new TestNode.TestService[]{
                new TestNode.MessageTrackingService(1, i),
                new TestNode.MessageTrackingService(2, i) })
            .start();
        systemTestWatcher.cluster(cluster);
        final int serviceCount = cluster.node(0).services().length;

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();

        TestNode.MessageTrackingService.delaySessionMessageProcessing(true);
        int messageCount = 0;
        final ExpandableArrayBuffer msgBuffer = cluster.msgBuffer();
        for (int i = 0; i < 2222; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);
        TestNode.MessageTrackingService.delaySessionMessageProcessing(false);

        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitServiceMessages(cluster, serviceCount, messageCount);
        stopConsensusModulesAndServices(cluster);

        final File leaderClusterDir = leader.consensusModule().context().clusterDir();
        final RecordingLog.Entry leaderSnapshot = ClusterTool.findLatestValidSnapshot(leaderClusterDir);
        assertNotNull(leaderSnapshot);

        final MutableLong mutableNextSessionId = new MutableLong();
        final MutableLong mutableNextServiceSessionId = new MutableLong();
        final MutableLong mutableLogServiceSessionId = new MutableLong();
        final MutableInteger consensusModuleStateOffset = new MutableInteger();
        final IntArrayList pendingMessageOffsets = new IntArrayList();
        readSnapshotRecording(leader, leaderSnapshot.recordingId, new NoOpConsensusModuleSnapshotListener()
        {
            public void onLoadConsensusModuleState(
                final long nextSessionId,
                final long nextServiceSessionId,
                final long logServiceSessionId,
                final int pendingMessageCapacity,
                final DirectBuffer buffer,
                final int offset,
                final int length)
            {
                mutableNextSessionId.set(nextSessionId);
                mutableNextServiceSessionId.set(nextServiceSessionId);
                mutableLogServiceSessionId.set(logServiceSessionId);
                consensusModuleStateOffset.set(offset);
                assertEquals(MessageHeaderDecoder.ENCODED_LENGTH + ConsensusModuleDecoder.BLOCK_LENGTH, length);
            }

            public void onLoadPendingMessage(
                final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
            {
                pendingMessageOffsets.addInt(offset);
                assertEquals(
                    MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.BLOCK_LENGTH + SIZE_OF_INT,
                    length);
            }
        });
        assertNotEquals(0, consensusModuleStateOffset.get());
        assertNotEquals(0, pendingMessageOffsets.size());

        final long expectedNextSessionId = mutableNextSessionId.get();
        final long expectedLogServiceSessionId = mutableLogServiceSessionId.get();
        final long invalidNextSessionId = Long.MAX_VALUE;
        assertNotEquals(mutableNextServiceSessionId.get(), invalidNextSessionId);

        modifySnapshot(
            leader,
            leaderSnapshot,
            consensusModuleStateOffset,
            pendingMessageOffsets,
            invalidNextSessionId);

        final ConsensusModuleSnapshotPendingServiceMessagesPatch snapshotPatch =
            new ConsensusModuleSnapshotPendingServiceMessagesPatch();
        assertTrue(snapshotPatch.execute(leaderClusterDir));

        final MutableBoolean onLoadConsensusModuleState = new MutableBoolean();
        final MutableInteger onLoadPendingMessageCount = new MutableInteger();
        readSnapshotRecording(leader, leaderSnapshot.recordingId, new NoOpConsensusModuleSnapshotListener()
        {
            long nextClusterSessionId = expectedLogServiceSessionId + 1;

            public void onLoadConsensusModuleState(
                final long nextSessionId,
                final long nextServiceSessionId,
                final long logServiceSessionId,
                final int pendingMessageCapacity,
                final DirectBuffer buffer,
                final int offset,
                final int length)
            {
                assertEquals(expectedNextSessionId, nextSessionId);
                assertEquals(expectedLogServiceSessionId, logServiceSessionId);
                assertEquals(expectedLogServiceSessionId + 1 + pendingMessageOffsets.size(), nextServiceSessionId);
                onLoadConsensusModuleState.set(true);
            }

            public void onLoadPendingMessage(
                final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
            {
                assertEquals(nextClusterSessionId++, clusterSessionId, "Invalid pending message header!");
                onLoadPendingMessageCount.increment();
            }
        });
        assertTrue(onLoadConsensusModuleState.get());
        assertEquals(pendingMessageOffsets.size(), onLoadPendingMessageCount.get());

        cluster.stopAllNodes();
        cluster.restartAllNodes(false);
        cluster.connectClient();

        for (int i = 0; i < 10; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitServiceMessages(cluster, serviceCount, messageCount);
    }

    private static void awaitServiceMessages(final TestCluster cluster, final int serviceCount, final int messageCount)
    {
        for (int i = 0; i < 3; i++)
        {
            final TestNode node = cluster.node(i);
            final TestNode.TestService[] services = node.services();
            for (final TestNode.TestService service : services)
            {
                // 1 client message + 3 service messages x number of services
                cluster.awaitServiceMessageCount(node, service, messageCount + (messageCount * 3 * serviceCount));
                // 2 timers x number of services
                cluster.awaitTimerEventCount(node, service, messageCount * 2 * serviceCount);
            }
        }
    }

    private static void stopConsensusModulesAndServices(final TestCluster cluster)
    {
        for (int i = 0; i < 3; i++)
        {
            final TestNode node = cluster.node(i);
            if (null != node)
            {
                node.stopServiceContainers();
                node.consensusModule().close();
            }
        }
    }

    private static void modifySnapshot(
        final TestNode leader,
        final RecordingLog.Entry leaderSnapshot,
        final MutableInteger consensusModuleStateOffset,
        final IntArrayList pendingMessageOffsets,
        final long invalidNextServiceSessionId)
    {
        final ArrayList<File> segmentFiles = listSegmentFiles(leader, leaderSnapshot.recordingId);
        assertEquals(1, segmentFiles.size());
        final MappedByteBuffer mappedByteBuffer =
            IoUtil.mapExistingFile(segmentFiles.get(0), "snapshot file");
        try
        {
            final UnsafeBuffer snapshotBuffer = new UnsafeBuffer(mappedByteBuffer);
            final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

            // Set the ConsensusModuleState values
            final ConsensusModuleEncoder consensusModuleEncoder = new ConsensusModuleEncoder();
            consensusModuleEncoder
                .wrapAndApplyHeader(snapshotBuffer, consensusModuleStateOffset.get(), messageHeaderEncoder)
                .nextServiceSessionId(invalidNextServiceSessionId);

            // Now randomize clusterSessionId of every pending service message
            final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
            pendingMessageOffsets.forEachInt((offset) -> sessionMessageHeaderEncoder
                .wrapAndApplyHeader(snapshotBuffer, offset, messageHeaderEncoder)
                .clusterSessionId(ThreadLocalRandom.current().nextLong()));
        }
        finally
        {
            IoUtil.unmap(mappedByteBuffer);
        }
    }

    private static void readSnapshotRecording(
        final TestNode node,
        final long recordingId,
        final ConsensusModuleSnapshotListener snapshotListener)
    {
        try (
            Aeron aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(node.mediaDriver().aeronDirectoryName()));
            AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                .controlRequestChannel(IPC_CHANNEL)
                .controlResponseChannel(IPC_CHANNEL)
                .aeron(aeron)))
        {
            replayLocalSnapshotRecording(
                aeron,
                archive,
                recordingId,
                snapshotListener);
        }
    }

    private static ArrayList<File> listSegmentFiles(final TestNode node, final long recordingId)
    {
        final File[] files = node.archive().context().archiveDir().listFiles();
        assertNotNull(files);
        final String segmentFileNamePrefix = recordingId + "-";
        final ArrayList<File> segmentFiles = new ArrayList<>();
        for (final File file : files)
        {
            if (null != file)
            {
                final String fileName = file.getName();
                if (fileName.startsWith(segmentFileNamePrefix) && fileName.endsWith(".rec"))
                {
                    segmentFiles.add(file);
                }
            }
        }
        return segmentFiles;
    }

    private static class NoOpConsensusModuleSnapshotListener implements ConsensusModuleSnapshotListener
    {
        public void onLoadBeginSnapshot(
            final int appVersion,
            final TimeUnit timeUnit,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadConsensusModuleState(
            final long nextSessionId,
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int pendingMessageCapacity,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadClusterMembers(
            final int memberId,
            final int highMemberId,
            final String clusterMembers,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadPendingMessage(
            final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
        {
        }

        public void onLoadClusterSession(
            final long clusterSessionId,
            final long correlationId,
            final long openedLogPosition,
            final long timeOfLastActivity,
            final CloseReason closeReason,
            final int responseStreamId,
            final String responseChannel,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadTimer(
            final long correlationId,
            final long deadline,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadPendingMessageTracker(
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int pendingMessageCapacity,
            final int serviceId,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadEndSnapshot(final DirectBuffer buffer, final int offset, final int length)
        {
        }
    }
}

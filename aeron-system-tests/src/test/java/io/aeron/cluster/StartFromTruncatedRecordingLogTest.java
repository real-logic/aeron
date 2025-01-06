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

import io.aeron.archive.ArchiveMarkFile;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.IoUtil;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.MutableInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.aeron.cluster.RecordingLog.RECORDING_LOG_FILE_NAME;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SlowTest
@ExtendWith({ InterruptingTestCallback.class, EventLogExtension.class })
class StartFromTruncatedRecordingLogTest
{
    private static final int MESSAGE_COUNT = 10;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private final MutableInteger responseCount = new MutableInteger();
    private TestCluster cluster;

    @Test
    @InterruptAfter(30)
    void shouldBeAbleToStartClusterFromTruncatedRecordingLog() throws IOException
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        restartClusterWithTruncatedRecordingLog();
        assertClusterIsOperational();

        restartClusterWithTruncatedRecordingLog();
        assertClusterIsOperational();

        restartClusterWithTruncatedRecordingLog();
        assertClusterIsOperational();
    }

    private void restartClusterWithTruncatedRecordingLog() throws IOException
    {
        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();
        final int leaderMemberId = leader.index();
        final int followerMemberIdA = cluster.followers().get(0).index();
        final int followerMemberIdB = cluster.followers().get(1).index();

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);
        cluster.awaitNeutralControlToggle(leader);

        cluster.stopNode(leader);
        cluster.awaitSnapshotCount(1);

        cluster.stopAllNodes();

        truncateRecordingLogAndDeleteMarkFiles(leaderMemberId);
        truncateRecordingLogAndDeleteMarkFiles(followerMemberIdA);
        truncateRecordingLogAndDeleteMarkFiles(followerMemberIdB);

        cluster.restartAllNodes(false);
    }

    private void assertClusterIsOperational()
    {
        cluster.awaitLeader();
        cluster.connectClient();

        final int initialCount = responseCount.get();
        cluster.sendMessages(MESSAGE_COUNT);
        cluster.awaitResponseMessageCount(MESSAGE_COUNT + initialCount);
        responseCount.addAndGet(MESSAGE_COUNT);
    }

    private void truncateRecordingLogAndDeleteMarkFiles(final int index) throws IOException
    {
        final File consensusModuleDataDir = cluster.node(index).consensusModule().context().clusterDir();
        final File archiveDataDir = cluster.node(index).archive().context().archiveDir();
        final String baseDirName = consensusModuleDataDir.getParentFile().getAbsolutePath();

        final File tmpRecordingFile = new File(baseDirName, RECORDING_LOG_FILE_NAME);
        deleteFile(tmpRecordingFile);
        deleteFile(new File(archiveDataDir, ArchiveMarkFile.FILENAME));
        deleteFile(new File(consensusModuleDataDir, ClusterMarkFile.FILENAME));

        try (RecordingLog recordingLog = new RecordingLog(consensusModuleDataDir, false))
        {
            final RecordingLog.Entry lastTermEntry = recordingLog.findLastTerm();
            if (null == lastTermEntry)
            {
                throw new IllegalStateException("no term found in recording log");
            }

            try (RecordingLog newRecordingLog = new RecordingLog(new File(baseDirName), true))
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

        try (RecordingLog copiedRecordingLog = new RecordingLog(consensusModuleDataDir, false))
        {
            final LongHashSet recordingIds = new LongHashSet();
            copiedRecordingLog.entries().stream().mapToLong((e) -> e.recordingId).forEach(recordingIds::add);
            final Predicate<Path> filterPredicate = (p) -> p.getFileName().toString().endsWith(".rec");

            try (Stream<Path> segments = Files.list(archiveDataDir.toPath()).filter(filterPredicate))
            {
                segments
                    .filter(
                        (p) ->
                        {
                            final String fileName = p.getFileName().toString();
                            final long recordingId = Long.parseLong(fileName.split("-")[0]);

                            return !recordingIds.contains(recordingId);
                        })
                    .map(Path::toFile).forEach(StartFromTruncatedRecordingLogTest::deleteFile);
            }

            assertTrue(copiedRecordingLog.entries().size() <= 3);
        }
    }

    private static void appendServiceSnapshot(
        final RecordingLog oldRecordingLog, final RecordingLog newRecordingLog, final int serviceId)
    {
        final RecordingLog.Entry snapshot = oldRecordingLog.getLatestSnapshot(serviceId);
        assertNotNull(snapshot);

        newRecordingLog.appendSnapshot(
            snapshot.recordingId,
            snapshot.leadershipTermId,
            snapshot.termBaseLogPosition,
            snapshot.logPosition,
            snapshot.timestamp,
            snapshot.serviceId);
    }

    private static void deleteFile(final File file)
    {
        IoUtil.delete(file, false);
    }
}

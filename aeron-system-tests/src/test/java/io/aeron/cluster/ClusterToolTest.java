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

import io.aeron.test.ClusterTestWatcher;
import io.aeron.test.SlowTest;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static io.aeron.test.cluster.TestCluster.TEST_CLUSTER_DEFAULT_LOG_FILTER;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
class ClusterToolTest
{
    @RegisterExtension
    final ClusterTestWatcher clusterTestWatcher = new ClusterTestWatcher();

    @AfterEach
    void tearDown()
    {
        assertEquals(
            0, clusterTestWatcher.errorCount(TEST_CLUSTER_DEFAULT_LOG_FILTER), "Errors observed in cluster test");
    }

    @Test
    @Timeout(30)
    void shouldHandleSnapshotOnLeaderOnly()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final long initialSnapshotCount = leader.consensusModule().context().snapshotCounter().get();
        final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();

        assertTrue(ClusterTool.snapshot(
            leader.consensusModule().context().clusterDir(),
            capturingPrintStream.resetAndGetPrintStream()));

        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString("SNAPSHOT applied successfully"));

        final long expectedSnapshotCount = initialSnapshotCount + 1;
        cluster.awaitSnapshotCount(expectedSnapshotCount);

        for (final TestNode follower : cluster.followers())
        {
            assertFalse(ClusterTool.snapshot(
                follower.consensusModule().context().clusterDir(),
                capturingPrintStream.resetAndGetPrintStream()));

            assertThat(
                capturingPrintStream.flushAndGetContent(),
                containsString("Current node is not the leader"));
        }
    }

    @Test
    @Timeout(30)
    void shouldNotSnapshotWhenSuspendedOnly()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final long initialSnapshotCount = leader.consensusModule().context().snapshotCounter().get();
        final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();

        assertTrue(ClusterTool.suspend(
            leader.consensusModule().context().clusterDir(),
            capturingPrintStream.resetAndGetPrintStream()));

        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString("SUSPEND applied successfully"));

        assertFalse(ClusterTool.snapshot(
            leader.consensusModule().context().clusterDir(),
            capturingPrintStream.resetAndGetPrintStream()));

        final String expectedMessage =
            "Unable to SNAPSHOT as the state of the consensus module is SUSPENDED, but needs to be ACTIVE";
        assertThat(capturingPrintStream.flushAndGetContent(), containsString(expectedMessage));

        assertEquals(initialSnapshotCount, leader.consensusModule().context().snapshotCounter().get());
    }

    @Test
    @Timeout(30)
    void shouldSuspendAndResume()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();

        assertTrue(ClusterTool.suspend(
            leader.consensusModule().context().clusterDir(),
            capturingPrintStream.resetAndGetPrintStream()));

        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString("SUSPEND applied successfully"));

        assertTrue(ClusterTool.resume(
            leader.consensusModule().context().clusterDir(),
            capturingPrintStream.resetAndGetPrintStream()));

        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString("RESUME applied successfully"));
    }

    @Test
    void shouldFailIfMarkFileUnavailable(final @TempDir Path emptyClusterDir)
    {
        final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();

        assertFalse(ClusterTool.snapshot(emptyClusterDir.toFile(), capturingPrintStream.resetAndGetPrintStream()));
        assertThat(
            capturingPrintStream.flushAndGetContent(),
            containsString("cluster-mark.dat does not exist"));
    }

    @Test
    void sortRecordingLogIsANoOpIfRecordLogIsEmpty(final @TempDir Path emptyClusterDir) throws IOException
    {
        final File clusterDir = emptyClusterDir.toFile();
        final Path logFile = emptyClusterDir.resolve(RecordingLog.RECORDING_LOG_FILE_NAME);

        final boolean result = ClusterTool.sortRecordingLog(clusterDir);

        assertFalse(result);
        assertArrayEquals(new byte[0], Files.readAllBytes(logFile));
    }

    @Test
    void sortRecordingLogIsANoOpIfRecordLogIsAlreadySorted(final @TempDir Path emptyClusterDir) throws IOException
    {
        final File clusterDir = emptyClusterDir.toFile();
        final Path logFile = emptyClusterDir.resolve(RecordingLog.RECORDING_LOG_FILE_NAME);
        try (RecordingLog recordingLog = new RecordingLog(clusterDir))
        {
            recordingLog.appendTerm(21, 0, 100, 100);
            recordingLog.appendSnapshot(0, 0, 0, 0, 200, 0);
            recordingLog.appendTerm(21, 1, 1024, 200);
        }

        final byte[] originalBytes = Files.readAllBytes(logFile);
        assertNotEquals(0, originalBytes.length);

        final boolean result = ClusterTool.sortRecordingLog(clusterDir);

        assertFalse(result);
        assertArrayEquals(originalBytes, Files.readAllBytes(logFile));
    }

    @Test
    void sortRecordingLogShouldRearrangeDataOnDisc(final @TempDir Path emptyClusterDir) throws IOException
    {
        final File clusterDir = emptyClusterDir.toFile();
        final Path logFile = emptyClusterDir.resolve(RecordingLog.RECORDING_LOG_FILE_NAME);
        try (RecordingLog recordingLog = new RecordingLog(clusterDir))
        {
            recordingLog.appendTerm(21, 2, 100, 100);
            recordingLog.appendSnapshot(0, 0, 0, 0, 200, 0);
            recordingLog.appendTerm(21, 1, 1024, 200);
            recordingLog.appendSnapshot(1, 2, 50, 60, 42, 89);
        }

        final byte[] originalBytes = Files.readAllBytes(logFile);
        assertNotEquals(0, originalBytes.length);

        final boolean result = ClusterTool.sortRecordingLog(clusterDir);

        assertTrue(result);
        assertFalse(Arrays.equals(originalBytes, Files.readAllBytes(logFile)));
        assertArrayEquals(new String[]{ RecordingLog.RECORDING_LOG_FILE_NAME }, clusterDir.list());
        try (RecordingLog recordingLog = new RecordingLog(clusterDir))
        {
            final List<RecordingLog.Entry> entries = recordingLog.entries();
            for (int i = entries.size() - 1; i >= 0; i--)
            {
                assertEquals(i, entries.get(i).entryIndex);
            }
        }
    }

    static class CapturingPrintStream
    {
        private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        private final PrintStream printStream = new PrintStream(byteArrayOutputStream);

        PrintStream resetAndGetPrintStream()
        {
            byteArrayOutputStream.reset();
            return printStream;
        }

        String flushAndGetContent()
        {
            printStream.flush();
            try
            {
                return byteArrayOutputStream.toString(US_ASCII.name());
            }
            catch (final UnsupportedEncodingException ex)
            {
                throw new RuntimeException(ex);
            }
        }
    }
}

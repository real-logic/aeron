/*
 *  Copyright 2014-2020 Real Logic Limited.
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

import io.aeron.test.SlowTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Path;

import static io.aeron.Aeron.NULL_VALUE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
class ClusterToolTest
{
    private final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();

    @Test
    void shouldHandleSnapshotOnLeaderOnly()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();
                final long initialSnapshotCount = cluster.countRecordingLogSnapshots(leader);

                assertTrue(ClusterTool.snapshot(
                    leader.consensusModule().context().clusterDir(),
                    capturingPrintStream.resetAndGetPrintStream()));

                assertThat(
                    capturingPrintStream.flushAndGetContent(US_ASCII),
                    containsString("SNAPSHOT applied successfully"));

                assertEquals(initialSnapshotCount + 1, cluster.countRecordingLogSnapshots(leader));

                for (final TestNode follower : cluster.followers())
                {
                    assertFalse(ClusterTool.snapshot(
                        follower.consensusModule().context().clusterDir(),
                        capturingPrintStream.resetAndGetPrintStream()));

                    assertThat(
                        capturingPrintStream.flushAndGetContent(US_ASCII),
                        containsString("Current node is not the leader"));
                }
            }
        });
    }

    @Test
    void shouldNotSnapshotWhenSuspendedOnly()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();
                final long initialSnapshotCount = cluster.countRecordingLogSnapshots(leader);

                assertTrue(ClusterTool.suspend(
                    leader.consensusModule().context().clusterDir(),
                    capturingPrintStream.resetAndGetPrintStream()));

                assertThat(
                    capturingPrintStream.flushAndGetContent(US_ASCII),
                    containsString("SUSPEND applied successfully"));

                assertFalse(ClusterTool.snapshot(
                    leader.consensusModule().context().clusterDir(),
                    capturingPrintStream.resetAndGetPrintStream()));

                final String expectedMessage =
                    "Unable to SNAPSHOT as the state of the consensus module is SUSPENDED, but needs to be ACTIVE";
                assertThat(capturingPrintStream.flushAndGetContent(US_ASCII), containsString(expectedMessage));

                assertEquals(initialSnapshotCount, cluster.countRecordingLogSnapshots(leader));
            }
        });
    }

    @Test
    void shouldSuspendAndResume()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();

                assertTrue(ClusterTool.suspend(
                    leader.consensusModule().context().clusterDir(),
                    capturingPrintStream.resetAndGetPrintStream()));

                assertThat(
                    capturingPrintStream.flushAndGetContent(US_ASCII),
                    containsString("SUSPEND applied successfully"));

                assertTrue(ClusterTool.resume(
                    leader.consensusModule().context().clusterDir(),
                    capturingPrintStream.resetAndGetPrintStream()));

                assertThat(
                    capturingPrintStream.flushAndGetContent(US_ASCII),
                    containsString("RESUME applied successfully"));
            }
        });
    }

    @Test
    void failIfMarkFileUnavailable(final @TempDir Path emptyClusterDir)
    {
        assertFalse(ClusterTool.snapshot(emptyClusterDir.toFile(), capturingPrintStream.resetAndGetPrintStream()));
        assertThat(
            capturingPrintStream.flushAndGetContent(US_ASCII),
            containsString("cluster-mark.dat does not exist"));
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

        String flushAndGetContent(final Charset charset)
        {
            printStream.flush();
            return new String(byteArrayOutputStream.toByteArray(), charset);
        }
    }
}

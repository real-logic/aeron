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

import io.aeron.test.SlowTest;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Path;

import static io.aeron.Aeron.NULL_VALUE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
class ClusterToolTest
{
    @Test
    @Timeout(30)
    void shouldHandleSnapshotOnLeaderOnly()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
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
    }

    @Test
    @Timeout(30)
    void shouldNotSnapshotWhenSuspendedOnly()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
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
    }

    @Test
    @Timeout(30)
    void shouldSuspendAndResume()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
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

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

import io.aeron.Aeron;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.node.NodeStateHeaderEncoder;
import io.aeron.cluster.service.ClusterMarkFile;
import org.agrona.IoUtil;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class NodeStateFileTest
{
    private static final long V_1_42_X_CANDIDATE_TERM_ID = 982374;
    private static final long V_1_42_X_LOG_POSITION = 89723648762342L;
    private static final long V_1_42_X_TIMESTAMP_MS = 9878967687234L;

    private final int syncLevel = 1;

    @Test
    void shouldFailIfCreateNewFalseAndFileDoesNotExist(@TempDir final File clusterDir)
    {
        assertThrows(IOException.class, () -> new NodeStateFile(clusterDir, false, syncLevel));
    }

    @Test
    void shouldCreateIfCreateNewTrueAndFileDoesNotExist(@TempDir final File clusterDir) throws IOException
    {
        assertEquals(0, requireNonNull(clusterDir.list()).length);
        try (NodeStateFile ignore = new NodeStateFile(clusterDir, true, syncLevel))
        {
            requireNonNull(ignore);
            assertTrue(new File(clusterDir, NodeStateFile.FILENAME).exists());
        }
    }

    @Test
    void shouldHaveNullCandidateTermIdOnInitialCreation(@TempDir final File clusterDir) throws IOException
    {
        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            assertEquals(Aeron.NULL_VALUE, nodeStateFile.candidateTerm().candidateTermId());
        }
    }

    @Test
    void shouldPersistCandidateTermId(@TempDir final File clusterDir) throws Exception
    {
        final long candidateTermId = 832234;
        final long timestampMs = 324234;
        final long logPosition = 8923423;
        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            nodeStateFile.updateCandidateTermId(candidateTermId, logPosition, timestampMs);
        }

        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, false, syncLevel))
        {
            assertEquals(candidateTermId, nodeStateFile.candidateTerm().candidateTermId());
            assertEquals(timestampMs, nodeStateFile.candidateTerm().timestamp());
            assertEquals(logPosition, nodeStateFile.candidateTerm().logPosition());
        }
    }

    @Test
    void shouldThrowIfVersionMismatch(@TempDir final File clusterDir) throws IOException
    {
        try (NodeStateFile ignore = new NodeStateFile(clusterDir, true, syncLevel))
        {
            requireNonNull(ignore);
        }

        final int invalidVersion = SemanticVersion.compose(
            ClusterMarkFile.MAJOR_VERSION + 1, ClusterMarkFile.MINOR_VERSION, ClusterMarkFile.PATCH_VERSION);
        forceVersion(clusterDir, invalidVersion);

        try (NodeStateFile ignore = new NodeStateFile(clusterDir, false, syncLevel))
        {
            requireNonNull(ignore);
            fail("ClusterException should have been thrown");
        }
        catch (final ClusterException ignore)
        {
        }
    }

    @Test
    void shouldProposeNewMaxTermId(@TempDir final File clusterDir) throws IOException
    {
        final long nextCandidateTermId;
        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            nodeStateFile.updateCandidateTermId(5, 10, 10);

            nextCandidateTermId = nodeStateFile.candidateTerm().candidateTermId() + 1;
            assertEquals(nextCandidateTermId, nodeStateFile.proposeMaxCandidateTermId(nextCandidateTermId, 20, 20));
            final long tooLowCandidateTermId = nodeStateFile.candidateTerm().candidateTermId() - 1;
            assertEquals(nextCandidateTermId, nodeStateFile.proposeMaxCandidateTermId(tooLowCandidateTermId, 30, 30));
        }

        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, false, syncLevel))
        {
            assertEquals(nextCandidateTermId, nodeStateFile.candidateTerm().candidateTermId());
        }
    }

    /**
     * Old node state file created using 1.42.x format with the following code.
     * <code>
     * try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
     * {
     *     nodeStateFile.updateCandidateTermId(
     *         V_1_42_X_CANDIDATE_TERM_ID, V_1_42_X_LOG_POSITION, V_1_42_X_TIMESTAMP_MS);
     *     nodeStateFile.updateClusterMembers(
     *         10001, 10002, 10003,
     *         "10001,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010" +
     *         "10002,localhost:20111,localhost:20221,localhost:20331,localhost:20441,localhost:8011" +
     *         "10003,localhost:20112,localhost:20222,localhost:20332,localhost:20442,localhost:8012");
     * }
     * </code>
     *
     * @param clusterDir temporary cluster directory
     * @throws IOException if an I/O exception occurs
     */
    @Test
    void shouldHandleNodeStateFileCreatedWithEarlierVersion(@TempDir final File clusterDir) throws IOException
    {
        final URL resource = this.getClass().getResource("/v1_42_x/node-state.dat");
        Files.copy(requireNonNull(resource).openStream(), new File(clusterDir, NodeStateFile.FILENAME).toPath());
        final long candidateTermId = 10L;
        final long logPosition = 20L;
        final long timestampMs = 30L;
        final long newCandidateTermId = candidateTermId + 100;

        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, false, syncLevel))
        {
            final NodeStateFile.CandidateTerm candidateTerm = nodeStateFile.candidateTerm();
            assertEquals(V_1_42_X_CANDIDATE_TERM_ID, candidateTerm.candidateTermId());
            assertEquals(V_1_42_X_LOG_POSITION, candidateTerm.logPosition());
            assertEquals(V_1_42_X_TIMESTAMP_MS, candidateTerm.timestamp());

            nodeStateFile.updateCandidateTermId(candidateTermId, logPosition, timestampMs);
            assertEquals(candidateTermId, candidateTerm.candidateTermId());
            assertEquals(logPosition, candidateTerm.logPosition());
            assertEquals(timestampMs, candidateTerm.timestamp());

            final long candidateTermIdPostPropose = nodeStateFile.proposeMaxCandidateTermId(
                newCandidateTermId, logPosition, timestampMs);
            assertEquals(newCandidateTermId, candidateTermIdPostPropose);
            assertEquals(newCandidateTermId, candidateTerm.candidateTermId());
        }
    }

    private void forceVersion(final File clusterDir, final int semanticVersion)
    {
        MappedByteBuffer buffer = null;
        try
        {
            buffer = IoUtil.mapExistingFile(
                new File(clusterDir, NodeStateFile.FILENAME), "test node state file");
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(buffer);
            unsafeBuffer.putInt(NodeStateHeaderEncoder.versionEncodingOffset(), semanticVersion);
        }
        finally
        {
            if (null != buffer)
            {
                IoUtil.unmap(buffer);
            }
        }
    }
}
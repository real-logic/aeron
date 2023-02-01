/*
 * Copyright 2014-2023 Real Logic Limited.
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
import java.nio.MappedByteBuffer;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class NodeStateFileTest
{
    private final int syncLevel = 1;

    @Test
    void shouldFailIfCreateNewFalseAndFileDoesNotExist(@TempDir final File clusterDir)
    {
        assertThrows(IOException.class, () -> new NodeStateFile(clusterDir, false, syncLevel));
    }

    @Test
    void shouldCreateIfCreateNewTrueAndFileDoesNotExist(@TempDir final File clusterDir) throws IOException
    {
        assertEquals(0, Objects.requireNonNull(clusterDir.list()).length);
        new NodeStateFile(clusterDir, true, syncLevel);
        assertTrue(new File(clusterDir, NodeStateFile.FILENAME).exists());
    }

    @Test
    void shouldHaveNullCandidateTermIdOnInitialCreation(@TempDir final File clusterDir) throws IOException
    {
        final NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel);
        assertEquals(Aeron.NULL_VALUE, nodeStateFile.candidateTerm().candidateTermId());
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
            Objects.requireNonNull(ignore);
        }

        final int invalidVersion = SemanticVersion.compose(
            ClusterMarkFile.MAJOR_VERSION + 1, ClusterMarkFile.MINOR_VERSION, ClusterMarkFile.PATCH_VERSION);
        forceVersion(clusterDir, invalidVersion);

        assertThrows(ClusterException.class, () -> new NodeStateFile(clusterDir, false, syncLevel));
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

    private void forceVersion(final File clusterDir, final int semanticVersion)
    {
        final MappedByteBuffer buffer = IoUtil.mapExistingFile(
            new File(clusterDir, NodeStateFile.FILENAME), "test node state file");
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(buffer);
        unsafeBuffer.putInt(NodeStateHeaderEncoder.versionEncodingOffset(), semanticVersion);
    }
}
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class NodeStateFileTest
{
    @Test
    void shouldFailIfCreateNewFalseAndFileDoesNotExist(@TempDir final File archiveDir)
    {
        assertThrows(IOException.class, () -> new NodeStateFile(archiveDir, false));
    }

    @Test
    void shouldCreateIfCreateNewTrueAndFileDoesNotExist(@TempDir final File archiveDir) throws IOException
    {
        assertEquals(0, Objects.requireNonNull(archiveDir.list()).length);
        new NodeStateFile(archiveDir, true);
        assertTrue(new File(archiveDir, NodeStateFile.FILENAME).exists());
    }

    @Test
    void shouldPersistCandidateTermId(@TempDir final File archiveDir) throws Exception
    {
        final long candidateTermId = 832234;
        final long timestampMs = 324234;
        final long logPosition = 8923423;
        try (NodeStateFile nodeStateFile = new NodeStateFile(archiveDir, true))
        {
            nodeStateFile.updateCandidateTermId(candidateTermId, logPosition, timestampMs);
        }

        try (NodeStateFile nodeStateFile = new NodeStateFile(archiveDir, false))
        {
            assertEquals(candidateTermId, nodeStateFile.candidateTerm().candidateTermId());
            assertEquals(timestampMs, nodeStateFile.candidateTerm().timestamp());
            assertEquals(logPosition, nodeStateFile.candidateTerm().logPosition());
        }
    }
}
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
package io.aeron.cluster.service;

import io.aeron.cluster.codecs.mark.ClusterComponentType;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.AdditionalMatchers.and;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.*;

class ClusterMarkFileTest
{
    @TempDir
    private Path tempDir;

    @Test
    void saveExistingErrorsIsANoOpIfErrorBufferIsEmpty()
    {
        final File markFile = tempDir.resolve("mark.dat").toFile();
        final UnsafeBuffer errorBuffer = new UnsafeBuffer(new byte[0]);
        final ClusterComponentType componentType = ClusterComponentType.BACKUP;
        final PrintStream logger = mock(PrintStream.class);

        ClusterMarkFile.saveExistingErrors(
            markFile,
            errorBuffer,
            componentType,
            logger);

        verifyNoInteractions(logger);
    }

    @Test
    void saveExistingErrorsCreatesErrorFileInTheSameDirectoryAsTheCorrespondingMarkFile()
    {
        final File markFile = tempDir.resolve("mark.dat").toFile();
        final DistinctErrorLog errorLog =
            new DistinctErrorLog(new UnsafeBuffer(new byte[10 * 1024]), SystemEpochClock.INSTANCE);
        assertTrue(errorLog.record(new Exception("Just to test")));
        final ClusterComponentType componentType = ClusterComponentType.CONSENSUS_MODULE;
        final PrintStream logger = mock(PrintStream.class);

        ClusterMarkFile.saveExistingErrors(
            markFile,
            errorLog.buffer(),
            componentType,
            logger);

        final File[] files = tempDir.toFile().listFiles(
            (dir, name) -> name.endsWith("-error.log") && name.startsWith(componentType.name()));
        assertNotNull(files);
        assertEquals(1, files.length);

        verify(logger).println(and(startsWith("WARNING: existing errors saved to: "), endsWith("-error.log")));
        verifyNoMoreInteractions(logger);
    }
}
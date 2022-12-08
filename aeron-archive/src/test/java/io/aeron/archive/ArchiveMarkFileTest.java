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
package io.aeron.archive;

import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.SystemEpochClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class ArchiveMarkFileTest
{
    @Test
    void shouldUseMarkFileDirectory(final @TempDir File tempDir)
    {
        final File aeronDir = new File(tempDir, "aeron");
        final File archiveDir = new File(tempDir, "archive_dir");
        final File markFileDir = new File(tempDir, "mark-file-home");
        final File markFile = new File(markFileDir, ArchiveMarkFile.FILENAME);
        assertTrue(markFileDir.mkdirs());
        assertFalse(markFile.exists());

        try (ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(
            new Archive.Context()
            .aeronDirectoryName(aeronDir.getAbsolutePath())
            .archiveDir(archiveDir)
            .markFileDir(markFileDir)
            .controlChannel(AeronArchive.Configuration.localControlChannel())
            .localControlChannel(AeronArchive.Configuration.localControlChannel())
            .epochClock(SystemEpochClock.INSTANCE)))
        {
            assertNotNull(archiveMarkFile);
        }

        assertTrue(markFile.exists());
        assertFalse(archiveDir.exists());
    }
}

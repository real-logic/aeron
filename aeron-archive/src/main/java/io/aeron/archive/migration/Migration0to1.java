/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.archive.migration;

import io.aeron.archive.ArchiveMarkFile;
import org.agrona.CloseHelper;
import org.agrona.SemanticVersion;

import java.io.File;
import java.nio.channels.FileChannel;

public class Migration0to1 implements MigrationStep
{
    private static final int MINIMUM_VERSION = SemanticVersion.compose(1, 0, 0);

    public int minimumVersion()
    {
        return MINIMUM_VERSION;
    }

    public void migrate(final ArchiveMarkFile markFile, final File archiveDir)
    {
        final FileChannel migrationTimestampFile = MigrationUtils.createMigrationTimestampFile(
            archiveDir, markFile.decoder().version(), MINIMUM_VERSION);

        // TODO: rename segment files based on start position of each file

        markFile.encoder().version(MINIMUM_VERSION);

        CloseHelper.close(migrationTimestampFile);
    }

    @Override
    public String toString()
    {
        return "to " + minimumVersion() + " Major " + SemanticVersion.major(minimumVersion()) + " Minor " +
            SemanticVersion.minor(minimumVersion()) + " Patch " + SemanticVersion.patch(minimumVersion());
    }
}

/*
 * Copyright 2014-2020 Real Logic Limited.
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

import org.agrona.LangUtil;
import org.agrona.SemanticVersion;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;

import static io.aeron.archive.MigrationUtils.fullVersionString;

class ArchiveMigration_2_3 implements ArchiveMigrationStep
{
    private static final int MINIMUM_VERSION = SemanticVersion.compose(3, 0, 0);

    public int minimumVersion()
    {
        return MINIMUM_VERSION;
    }

    public void migrate(
        final PrintStream stream,
        final ArchiveMarkFile markFile,
        final Catalog catalog,
        final File archiveDir)
    {
        try (FileChannel ignore = MigrationUtils.createMigrationTimestampFile(
            archiveDir, markFile.decoder().version(), minimumVersion()))
        {
            // FIXME: Migrate Catalog file...

            markFile.encoder().version(minimumVersion());
            catalog.updateVersion(minimumVersion());
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public String toString()
    {
        return "to " + fullVersionString(minimumVersion());
    }
}

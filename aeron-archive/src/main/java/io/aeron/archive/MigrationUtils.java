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
package io.aeron.archive;

import org.agrona.LangUtil;
import org.agrona.SemanticVersion;

import java.io.File;
import java.nio.channels.FileChannel;

import static java.nio.file.StandardOpenOption.*;

final class MigrationUtils
{
    private static final String MIGRATION_TIMESTAMP_FILE_PREFIX = "migration-";
    private static final String MIGRATION_TIMESTAMP_FILE_SUFFIX = ".dat";

    private MigrationUtils()
    {
    }

    static FileChannel createMigrationTimestampFile(
        final File directory, final int fromVersion, final int toVersion)
    {
        final String filename = migrationTimestampFileName(fromVersion, toVersion);
        final File timestampFile = new File(directory, filename);

        FileChannel fileChannel = null;

        try
        {
            fileChannel = FileChannel.open(timestampFile.toPath(), CREATE_NEW, READ, WRITE, SPARSE);
            fileChannel.force(true);
        }
        catch (final Exception ex)
        {
            System.err.println("Could not create migration timestamp file:" + timestampFile);
            LangUtil.rethrowUnchecked(ex);
        }

        return fileChannel;
    }

    static String migrationTimestampFileName(final int fromVersion, final int toVersion)
    {
        return MIGRATION_TIMESTAMP_FILE_PREFIX + fromVersion + "-to-" + toVersion + MIGRATION_TIMESTAMP_FILE_SUFFIX;
    }

    static String fullVersionString(final int version)
    {
        return version +
            "(Major " + SemanticVersion.major(version) +
            " Minor " + SemanticVersion.minor(version) +
            " Patch " + SemanticVersion.patch(version) + ")";
    }
}

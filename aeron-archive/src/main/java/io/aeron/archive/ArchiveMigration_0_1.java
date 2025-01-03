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

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderEncoder;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.AsciiEncoding;
import org.agrona.LangUtil;
import org.agrona.SemanticVersion;
import org.agrona.collections.ArrayUtil;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

import static io.aeron.archive.MigrationUtils.fullVersionString;
import static io.aeron.archive.codecs.RecordingState.INVALID;

class ArchiveMigration_0_1 implements ArchiveMigrationStep
{
    private static final int MINIMUM_VERSION = SemanticVersion.compose(1, 0, 0);

    /**
     * {@inheritDoc}
     */
    public int minimumVersion()
    {
        return MINIMUM_VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("try")
    public void migrate(
        final PrintStream stream,
        final ArchiveMarkFile markFile,
        final Catalog catalog,
        final File archiveDir)
    {
        try (FileChannel ignore = MigrationUtils.createMigrationTimestampFile(
            archiveDir, catalog.version(), minimumVersion()))
        {
            catalog.forEach(
                (recordingDescriptorOffset, headerEncoder, headerDecoder, encoder, decoder) ->
                {
                    final String version0Prefix = decoder.recordingId() + "-";
                    final String version0Suffix = ".rec";
                    String[] segmentFiles = archiveDir.list(
                        (dir, filename) -> filename.startsWith(version0Prefix) && filename.endsWith(version0Suffix));

                    if (null == segmentFiles)
                    {
                        segmentFiles = ArrayUtil.EMPTY_STRING_ARRAY;
                    }

                    migrateRecording(
                        stream,
                        archiveDir,
                        segmentFiles,
                        version0Prefix,
                        version0Suffix,
                        headerEncoder,
                        headerDecoder,
                        encoder,
                        decoder);
                });

            markFile.encoder().version(minimumVersion());
            catalog.updateVersion(minimumVersion());
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void migrateRecording(
        final PrintStream stream,
        final File archiveDir,
        final String[] segmentFiles,
        final String prefix,
        final String suffix,
        final RecordingDescriptorHeaderEncoder headerEncoder,
        final RecordingDescriptorHeaderDecoder headerDecoder,
        final RecordingDescriptorEncoder encoder,
        final RecordingDescriptorDecoder decoder)
    {
        final long recordingId = decoder.recordingId();
        final long startPosition = decoder.startPosition();
        final long segmentLength = decoder.segmentFileLength();
        final long segmentBasePosition = startPosition - (startPosition & (segmentLength - 1));
        final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift((int)segmentLength);

        if (headerDecoder.state() == INVALID)
        {
            return;
        }

        stream.println(
            "(recordingId=" + recordingId + ") segmentBasePosition=" + segmentBasePosition + " " +
            "segmentLength=" + segmentLength + "(" + positionBitsToShift + ")");

        for (final String filename : segmentFiles)
        {
            final int length = filename.length();
            final int offset = prefix.length();
            final int remaining = length - offset - suffix.length();
            final long segmentIndex;

            if (remaining > 0)
            {
                try
                {
                    segmentIndex = AsciiEncoding.parseIntAscii(filename, offset, remaining);
                }
                catch (final Exception ex)
                {
                    stream.println(
                        "(recordingId=" + recordingId + ") ERR: malformed recording filename:" + filename);
                    throw ex;
                }

                final long segmentPosition = (segmentIndex << positionBitsToShift) + segmentBasePosition;
                final String newFilename = prefix + segmentPosition + suffix;

                final Path sourcePath = new File(archiveDir, filename).toPath();
                final Path targetPath = sourcePath.resolveSibling(newFilename);

                stream.println("(recordingId=" + recordingId + ") renaming " + sourcePath + " -> " + targetPath);

                try
                {
                    Files.move(sourcePath, targetPath);
                    Files.setLastModifiedTime(targetPath, FileTime.fromMillis(System.currentTimeMillis()));
                }
                catch (final Exception ex)
                {
                    stream.println(
                        "(recordingId=" + recordingId + ") ERR: could not rename filename: " +
                        sourcePath + " -> " + targetPath);
                    LangUtil.rethrowUnchecked(ex);
                }
            }
        }

        stream.println("(recordingId=" + recordingId + ") OK");
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "to " + fullVersionString(minimumVersion());
    }
}

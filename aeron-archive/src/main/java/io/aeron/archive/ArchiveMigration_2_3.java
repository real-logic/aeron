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

import org.agrona.*;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.aeron.archive.Archive.Configuration.CATALOG_FILE_NAME;
import static io.aeron.archive.Catalog.DESCRIPTOR_HEADER_LENGTH;
import static io.aeron.archive.Catalog.MAX_CATALOG_LENGTH;
import static io.aeron.archive.MigrationUtils.fullVersionString;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.*;

class ArchiveMigration_2_3 implements ArchiveMigrationStep
{
    private static final int MINIMUM_VERSION = SemanticVersion.compose(3, 0, 0);

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
        final int version = minimumVersion();
        try (FileChannel ignore = MigrationUtils.createMigrationTimestampFile(
            archiveDir, markFile.decoder().version(), version))
        {
            final File newFile = new File(archiveDir, CATALOG_FILE_NAME + ".updated");
            IoUtil.deleteIfExists(newFile);
            final Path updatedCatalogFile = newFile.toPath();

            try (FileChannel channel = FileChannel.open(updatedCatalogFile, READ, WRITE, CREATE_NEW))
            {
                final MappedByteBuffer mappedByteBuffer = channel.map(READ_WRITE, 0, MAX_CATALOG_LENGTH);
                mappedByteBuffer.order(LITTLE_ENDIAN);
                final int offset;
                try
                {
                    offset = migrateCatalogFile(catalog, version, mappedByteBuffer);
                }
                finally
                {
                    BufferUtil.free(mappedByteBuffer);
                }

                channel.truncate(offset); // trim file to actual length used
            }

            catalog.close();

            final Path catalogFilePath = updatedCatalogFile.resolveSibling(CATALOG_FILE_NAME);
            Files.delete(catalogFilePath);
            Files.move(updatedCatalogFile, catalogFilePath);

            markFile.encoder().version(version);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "to " + fullVersionString(minimumVersion());
    }

    private int migrateCatalogFile(final Catalog catalog, final int version, final MappedByteBuffer mappedByteBuffer)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(mappedByteBuffer);

        final int alignment = CACHE_LINE_LENGTH;

        // Create CatalogHeader for version 3.0.0
        final int catalogHeaderLength =
            writeCatalogHeader(buffer, version, catalog.nextRecordingId(), alignment);

        final MutableInteger offset = new MutableInteger(catalogHeaderLength);

        catalog.forEach(
            (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
            {
                final int strippedChannelLength = descriptorDecoder.strippedChannelLength();
                descriptorDecoder.skipStrippedChannel();

                final int originalChannelLength = descriptorDecoder.originalChannelLength();
                descriptorDecoder.skipOriginalChannel();

                final int sourceIdentityLength = descriptorDecoder.sourceIdentityLength();

                final int frameLength =
                    align(DESCRIPTOR_HEADER_LENGTH + 7 * SIZE_OF_LONG + 6 * SIZE_OF_INT +
                    SIZE_OF_INT + strippedChannelLength +
                    SIZE_OF_INT + originalChannelLength +
                    SIZE_OF_INT + sourceIdentityLength,
                    alignment);

                final DirectBuffer srcBuffer = headerDecoder.buffer();
                final int headerLength = headerDecoder.encodedLength();

                // Copy recording header
                int index = offset.get();
                buffer.putBytes(
                    index,
                    srcBuffer,
                    0,
                    headerLength);
                index += headerLength;

                // Correct length
                buffer.putInt(offset.get(), frameLength - headerLength, LITTLE_ENDIAN);

                // Copy recording descriptor
                buffer.putBytes(
                    index,
                    srcBuffer,
                    headerLength,
                    frameLength - headerLength);

                offset.addAndGet(frameLength);
            });

        return offset.get();
    }

    private int writeCatalogHeader(
        final UnsafeBuffer buffer, final int version, final long nextRecordingId, final int alignment)
    {
        final int catalogHeaderLength = 32;

        int index = 0;
        buffer.putInt(index, version, LITTLE_ENDIAN); // version
        index += SIZE_OF_INT;

        buffer.putInt(index, catalogHeaderLength, LITTLE_ENDIAN); // length
        index += SIZE_OF_INT;

        buffer.putLong(index, nextRecordingId, LITTLE_ENDIAN); // nextRecordingId
        index += SIZE_OF_LONG;

        buffer.putInt(index, alignment, LITTLE_ENDIAN); // alignment

        return catalogHeaderLength;
    }
}

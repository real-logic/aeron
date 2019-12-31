/*
 * Copyright 2019 Real Logic Ltd.
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

import io.aeron.archive.Archive.Context;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.checksum.Checksums;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static io.aeron.archive.Archive.Configuration.RECORD_CHECKSUM_PROP_NAME;
import static io.aeron.archive.Archive.Configuration.REPLAY_CHECKSUM_PROP_NAME;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class ArchiveContextTests
{
    @Test
    void recordChecksumReturnsNullIfNoSupplierWasConfigured()
    {
        final Context context = new Context();

        assertNull(context.recordChecksum());
    }

    @Test
    void recordChecksumUsesExplicitlyConfiguredSupplier()
    {
        final Context context = new Context();
        final Checksum checksum = (address, offset, length) -> -1;
        final Supplier<Checksum> supplier = () -> checksum;
        context.recordChecksumSupplier(supplier);

        assertSame(supplier, context.recordChecksumSupplier());
        assertSame(checksum, context.recordChecksum());
    }

    @Test
    void concludeRecordChecksumSupplierCreatesDefaultSupplierFromTheConfiguredClassName()
    {
        System.setProperty(RECORD_CHECKSUM_PROP_NAME, Checksums.crc32().getClass().getName());
        try
        {
            final Context context = new Context();
            context.concludeRecordChecksumSupplier();

            assertSame(Checksums.crc32(), context.recordChecksum());
        }
        finally
        {
            System.clearProperty(RECORD_CHECKSUM_PROP_NAME);
        }
    }

    @Test
    void concludeRecordChecksumSupplierDoesNotCreateADefaultSupplierIfOneIsAlreadyAssigned()
    {
        System.setProperty(RECORD_CHECKSUM_PROP_NAME, "test");
        try
        {
            final Context context = new Context();
            final Checksum checksum = (address, offset, length) -> -1;
            context.recordChecksumSupplier(() -> checksum);

            context.concludeRecordChecksumSupplier();

            assertSame(checksum, context.recordChecksum());
        }
        finally
        {
            System.clearProperty(RECORD_CHECKSUM_PROP_NAME);
        }
    }

    @Test
    void replayChecksumReturnsNullIfNoSupplierWasConfigured()
    {
        final Context context = new Context();

        assertNull(context.replayChecksum());
    }

    @Test
    void replayChecksumUsesExplicitlyConfiguredSupplier()
    {
        final Context context = new Context();
        final Checksum checksum = (address, offset, length) -> -1;
        final Supplier<Checksum> supplier = () -> checksum;
        context.replayChecksumSupplier(supplier);

        assertSame(supplier, context.replayChecksumSupplier());
        assertSame(checksum, context.replayChecksum());
    }

    @Test
    void concludeReplayChecksumSupplierCreatesDefaultSupplierFromTheConfiguredClassName()
    {
        System.setProperty(REPLAY_CHECKSUM_PROP_NAME, Checksums.crc32().getClass().getName());
        try
        {
            final Context context = new Context();
            context.concludeReplayChecksumSupplier();

            assertSame(Checksums.crc32(), context.replayChecksum());
        }
        finally
        {
            System.clearProperty(REPLAY_CHECKSUM_PROP_NAME);
        }
    }

    @Test
    void concludeReplayChecksumSupplierDoesNotCreateADefaultSupplierIfOneIsAlreadyAssigned()
    {
        System.setProperty(REPLAY_CHECKSUM_PROP_NAME, "test");
        try
        {
            final Context context = new Context();
            final Checksum checksum = (address, offset, length) -> -1;
            context.replayChecksumSupplier(() -> checksum);

            context.concludeReplayChecksumSupplier();

            assertSame(checksum, context.replayChecksum());
        }
        finally
        {
            System.clearProperty(REPLAY_CHECKSUM_PROP_NAME);
        }
    }
}

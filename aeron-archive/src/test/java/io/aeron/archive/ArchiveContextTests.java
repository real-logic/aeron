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
import io.aeron.archive.client.ArchiveException;
import org.junit.jupiter.api.Test;

import static io.aeron.archive.Archive.Configuration.*;
import static org.junit.jupiter.api.Assertions.*;

public class ArchiveContextTests
{
    @Test
    void recordingCrcEnabledDefaultValue()
    {
        final Context context = new Context();
        assertFalse(context.recordingCrcEnabled());
    }

    @Test
    void recordingCrcEnabledDefaultValueTakenFromTheProperty()
    {
        System.setProperty(RECORDING_CRC_ENABLED_PROP_NAME, "wrong value");
        try
        {
            final Context context = new Context();
            assertFalse(context.recordingCrcEnabled());
        }
        finally
        {
            System.clearProperty(RECORDING_CRC_ENABLED_PROP_NAME);
        }
    }

    @Test
    void recordingCrcEnabledDefaultValueTrue()
    {
        System.setProperty(RECORDING_CRC_ENABLED_PROP_NAME, "true");
        try
        {
            final Context context = new Context();
            assertTrue(context.recordingCrcEnabled());
        }
        finally
        {
            System.clearProperty(RECORDING_CRC_ENABLED_PROP_NAME);
        }
    }

    @Test
    void recordingCrcEnabled()
    {
        final Context context = new Context();
        context.recordingCrcEnabled(true);
        assertTrue(context.recordingCrcEnabled());
    }

    @Test
    void replayCrcEnabledDefaultValue()
    {
        final Context context = new Context();
        assertFalse(context.replayCrcEnabled());
    }

    @Test
    void replayCrcEnabledDefaultValueTakenFromTheProperty()
    {
        System.setProperty(REPLAY_CRC_ENABLED_PROP_NAME, "wrong value");
        try
        {
            final Context context = new Context();
            assertFalse(context.replayCrcEnabled());
        }
        finally
        {
            System.clearProperty(REPLAY_CRC_ENABLED_PROP_NAME);
        }
    }

    @Test
    void replayCrcEnabledDefaultValueTrue()
    {
        System.setProperty(REPLAY_CRC_ENABLED_PROP_NAME, "true");
        try
        {
            final Context context = new Context();
            assertTrue(context.replayCrcEnabled());
        }
        finally
        {
            System.clearProperty(REPLAY_CRC_ENABLED_PROP_NAME);
        }
    }

    @Test
    void replayCrcEnabled()
    {
        final Context context = new Context();
        context.replayCrcEnabled(true);
        assertTrue(context.replayCrcEnabled());
    }

    @Test
    void concludeRecordingChecksumSupplierThrowsArchiveExceptionIfSupplierIsAssignedButTheFlagIsFalse()
    {
        final Context context = new Context();
        context.recordingCrcEnabled(false);
        context.recordingCrcChecksumSupplier(Checksums::crc32);
        assertThrows(ArchiveException.class, context::concludeRecordingChecksumSupplier);
    }

    @Test
    void concludeRecordingChecksumSupplierAThrowsArchiveExceptionIfChecksumClassNameIsSetButFlagIsFalse()
    {
        System.setProperty(RECORDING_CRC_CHECKSUM_PROP_NAME, "abc");
        try
        {
            final Context context = new Context();
            context.recordingCrcEnabled(false);
            assertThrows(ArchiveException.class, context::concludeRecordingChecksumSupplier);
        }
        finally
        {
            System.clearProperty(RECORDING_CRC_CHECKSUM_PROP_NAME);
        }
    }

    @Test
    void concludeRecordingChecksumSupplierThrowsArchiveExceptionIfFlagIsTrueButChecksumClassIsNotConfigured()
    {
        final Context context = new Context();
        context.recordingCrcEnabled(true);
        assertThrows(ArchiveException.class, context::concludeRecordingChecksumSupplier);
    }

    @Test
    void concludeRecordingChecksumSupplierAssignsChecksumSupplierBasedOnTheConfiguredClassName()
    {
        System.setProperty(RECORDING_CRC_CHECKSUM_PROP_NAME, Checksums.crc32().getClass().getName());
        try
        {
            final Context context = new Context();
            context.recordingCrcEnabled(true);

            context.concludeRecordingChecksumSupplier();

            assertSame(Checksums.crc32(), context.recordingCrcChecksum());
        }
        finally
        {
            System.clearProperty(RECORDING_CRC_CHECKSUM_PROP_NAME);
        }
    }

    @Test
    void concludeRecordingChecksumSupplierUsesExplicitlyAssignedSupplier()
    {
        final Context context = new Context();
        context.recordingCrcEnabled(true);
        final Checksum myChecksum = (address, offset, length) -> 0;
        context.recordingCrcChecksumSupplier(() -> myChecksum);

        context.concludeRecordingChecksumSupplier();

        assertSame(myChecksum, context.recordingCrcChecksum());
    }

    @Test
    void recordingCrcChecksumThrowsNullPointerExceptionIfNoChecksumSupplierWasConfigured()
    {
        final Context context = new Context();
        context.recordingCrcEnabled(false);
        context.concludeRecordingChecksumSupplier();

        assertThrows(NullPointerException.class, context::recordingCrcChecksum);
    }

    @Test
    void concludeReplayChecksumSupplierThrowsArchiveExceptionIfSupplierIsAssignedButTheFlagIsFalse()
    {
        final Context context = new Context();
        context.replayCrcEnabled(false);
        context.replayCrcChecksumSupplier(Checksums::crc32);
        assertThrows(ArchiveException.class, context::concludeReplayChecksumSupplier);
    }

    @Test
    void concludeReplayChecksumSupplierAThrowsArchiveExceptionIfChecksumClassNameIsSetButFlagIsFalse()
    {
        System.setProperty(REPLAY_CRC_CHECKSUM_PROP_NAME, "abc");
        try
        {
            final Context context = new Context();
            context.replayCrcEnabled(false);
            assertThrows(ArchiveException.class, context::concludeReplayChecksumSupplier);
        }
        finally
        {
            System.clearProperty(REPLAY_CRC_CHECKSUM_PROP_NAME);
        }
    }

    @Test
    void concludeReplayChecksumSupplierThrowsArchiveExceptionIfFlagIsTrueButChecksumClassIsNotConfigured()
    {
        final Context context = new Context();
        context.replayCrcEnabled(true);
        assertThrows(ArchiveException.class, context::concludeReplayChecksumSupplier);
    }

    @Test
    void concludeReplayChecksumSupplierAssignsChecksumSupplierBasedOnTheConfiguredClassName()
    {
        System.setProperty(REPLAY_CRC_CHECKSUM_PROP_NAME, Checksums.crc32().getClass().getName());
        try
        {
            final Context context = new Context();
            context.replayCrcEnabled(true);

            context.concludeReplayChecksumSupplier();

            assertSame(Checksums.crc32(), context.replayCrcChecksum());
        }
        finally
        {
            System.clearProperty(REPLAY_CRC_CHECKSUM_PROP_NAME);
        }
    }

    @Test
    void concludeReplayChecksumSupplierUsesExplicitlyAssignedSupplier()
    {
        final Context context = new Context();
        context.replayCrcEnabled(true);
        final Checksum myChecksum = (address, offset, length) -> 0;
        context.replayCrcChecksumSupplier(() -> myChecksum);

        context.concludeReplayChecksumSupplier();

        assertSame(myChecksum, context.replayCrcChecksum());
    }

    @Test
    void replayCrcChecksumThrowsNullPointerExceptionIfNoChecksumSupplierWasConfigured()
    {
        final Context context = new Context();
        context.replayCrcEnabled(false);
        context.concludeReplayChecksumSupplier();

        assertThrows(NullPointerException.class, context::replayCrcChecksum);
    }
}

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
import org.junit.jupiter.api.Test;

import static io.aeron.archive.Archive.Configuration.RECORDING_CRC_ENABLED_PROP_NAME;
import static io.aeron.archive.Archive.Configuration.REPLAY_CRC_ENABLED_PROP_NAME;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}

/*
 * Copyright 2014-2023 Real Logic Limited.
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
package io.aeron.driver;

import io.aeron.driver.MediaDriver.Context;
import io.aeron.exceptions.ConfigurationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.aeron.driver.Configuration.*;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MAX_LENGTH;
import static org.junit.jupiter.api.Assertions.*;

class MediaDriverContextTest
{
    @Test
    void nakMulticastMaxBackoffNsDefaultValue()
    {
        final Context context = new Context();
        assertEquals(NAK_MAX_BACKOFF_DEFAULT_NS, context.nakMulticastMaxBackoffNs());
    }

    @Test
    void nakMulticastMaxBackoffNsValueFromSystemProperty()
    {
        System.setProperty(NAK_MULTICAST_MAX_BACKOFF_PROP_NAME, "333");
        try
        {
            final Context context = new Context();
            assertEquals(333, context.nakMulticastMaxBackoffNs());
        }
        finally
        {
            System.clearProperty(NAK_MULTICAST_MAX_BACKOFF_PROP_NAME);
        }
    }

    @Test
    void nakMulticastMaxBackoffNsExplicitValue()
    {
        final Context context = new Context();
        context.nakMulticastMaxBackoffNs(Long.MIN_VALUE);
        assertEquals(Long.MIN_VALUE, context.nakMulticastMaxBackoffNs());
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -5, 0, 1024 * 1024, 1024 * 1024 + 64 * 12 - 1 })
    void conductorBufferLengthMustBeWithinRange(final int length)
    {
        final Context context = new Context();
        context.conductorBufferLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("conductorBufferLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -5, 0, 1024 * 1024, 1024 * 1024 + 64 * 2 - 1 })
    void toClientsBufferLengthMustBeWithinRange(final int length)
    {
        final Context context = new Context();
        context.toClientsBufferLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("toClientsBufferLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { -76, 0, 1024 * 1024 - 1, 1024 * 1024 * 1024 })
    void counterValuesBufferLengthMustBeWithinRange(final int length)
    {
        final Context context = new Context();
        context.counterValuesBufferLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("counterValuesBufferLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { -76, 0, ERROR_BUFFER_LENGTH_DEFAULT - 1 })
    void errorBufferLengthMustBeWithinRange(final int length)
    {
        final Context context = new Context();
        context.errorBufferLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("errorBufferLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { -6, 0, 5, LOSS_REPORT_BUFFER_LENGTH_DEFAULT - 4096 })
    void lossReportBufferLengthMustBeWithinRange(final int length, final @TempDir Path temp) throws IOException
    {
        final Path aeronDir = temp.resolve("aeron");
        Files.createDirectories(aeronDir);

        final Context context = new Context();
        context.aeronDirectoryName(aeronDir.toString());
        context.lossReportBufferLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("lossReportBufferLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { -76, -3, TERM_MAX_LENGTH + 1 })
    void publicationTermWindowLengthMustBeWithinRange(final int length)
    {
        final Context context = new Context();
        context.publicationTermWindowLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("publicationTermWindowLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { -76, -3, TERM_MAX_LENGTH + 1 })
    void ipcPublicationTermWindowLengthMustBeWithinRange(final int length)
    {
        final Context context = new Context();
        context.ipcPublicationTermWindowLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("ipcPublicationTermWindowLength"));
    }
}

/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.agent;

import org.agrona.collections.ObjectHashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import java.util.EnumMap;
import java.util.Map;

import static io.aeron.agent.ConfigOption.*;
import static org.junit.jupiter.api.Assertions.*;

class ConfigOptionTest
{
    @Test
    void propertyNameShouldBeUnique()
    {
        final ObjectHashSet<String> properties = new ObjectHashSet<>();
        for (final ConfigOption option : values())
        {
            assertTrue(properties.add(option.propertyName()));
        }
    }

    @Test
    void shouldReadSystemProperties()
    {
        final EnumMap<ConfigOption, String> expectedValues = new EnumMap<>(ConfigOption.class);
        try
        {
            expectedValues.put(DISABLED_ARCHIVE_EVENT_CODES, "abc");
            expectedValues.put(LOG_FILENAME, "");
            expectedValues.put(ENABLED_CLUSTER_EVENT_CODES, "1,2,3");
            for (final Map.Entry<ConfigOption, String> entry : expectedValues.entrySet())
            {
                System.setProperty(entry.getKey().propertyName(), entry.getValue());
            }
            System.setProperty("ignore me", "1000");

            final EnumMap<ConfigOption, String> values = fromSystemProperties();

            assertEquals(expectedValues, values);
        }
        finally
        {
            System.clearProperty(DISABLED_ARCHIVE_EVENT_CODES.propertyName());
            System.clearProperty(LOG_FILENAME.propertyName());
            System.clearProperty(ENABLED_CLUSTER_EVENT_CODES.propertyName());
            System.clearProperty("ignore me");
        }
    }

    @Test
    void shouldReturnAnEmptyMapIfNoSystemPropertiesAreSet()
    {
        final EnumMap<ConfigOption, String> expectedValues = new EnumMap<>(ConfigOption.class);

        final EnumMap<ConfigOption, String> values = fromSystemProperties();

        assertEquals(expectedValues, values);
    }

    @Test
    void shouldThrowNullPointerExceptionIfConfigOptionsMapIsNull()
    {
        assertThrows(NullPointerException.class, () -> buildAgentArgs(null));
    }

    @Test
    void shouldReturnNullIfConfigOptionsMapIsEmpty()
    {
        assertNull(buildAgentArgs(new EnumMap<>(ConfigOption.class)));
    }

    @Test
    void shouldCombineConfigOptionsIntoAnAgentArgsString()
    {
        final EnumMap<ConfigOption, String> configOptions = new EnumMap<>(ConfigOption.class);
        configOptions.put(READER_CLASSNAME, "reader class");
        configOptions.put(DISABLED_CLUSTER_EVENT_CODES, "CANVASS_POSITION");
        configOptions.put(ENABLED_DRIVER_EVENT_CODES, "all");
        configOptions.put(LOG_FILENAME, "file.txt");
        configOptions.put(DISABLED_DRIVER_EVENT_CODES, "admin");

        final String agentArgs = buildAgentArgs(configOptions);

        assertEquals("LOG_FILENAME=file.txt|" +
            "READER_CLASSNAME=reader class|" +
            "ENABLED_DRIVER_EVENT_CODES=all|" +
            "DISABLED_DRIVER_EVENT_CODES=admin|" +
            "DISABLED_CLUSTER_EVENT_CODES=CANVASS_POSITION",
            agentArgs);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldThrowExceptionIfNullOrEmptyAgentArgs(final String agentArgs)
    {
        final IllegalArgumentException exception =
            assertThrows(IllegalArgumentException.class, () -> parseAgentArgs(agentArgs));
        assertEquals("cannot parse empty value", exception.getMessage());
    }

    @Test
    void shouldParseAgentArgsAsAConfigOptionsMap()
    {
        final EnumMap<ConfigOption, String> expectedOptions = new EnumMap<>(ConfigOption.class);
        expectedOptions.put(LOG_FILENAME, "log.out");
        expectedOptions.put(READER_CLASSNAME, "my reader");
        expectedOptions.put(ENABLED_DRIVER_EVENT_CODES, "all");
        expectedOptions.put(DISABLED_DRIVER_EVENT_CODES, "FRAME_IN,FRAME_OUT");
        expectedOptions.put(ENABLED_CLUSTER_EVENT_CODES, "all");
        expectedOptions.put(DISABLED_CLUSTER_EVENT_CODES, "CANVASS_POSITION,STATE_CHANGE");

        final EnumMap<ConfigOption, String> configOptions = parseAgentArgs(
            "||1600|abc|LOG_FILENAME=log.out|" +
            "ENABLED_CLUSTER_EVENT_CODES=all|" +
            "READER_CLASSNAME=my reader|" +
            "DISABLED_DRIVER_EVENT_CODES=FRAME_IN,FRAME_OUT|" +
            "ENABLED_DRIVER_EVENT_CODES=all|" +
            "DISABLED_CLUSTER_EVENT_CODES=CANVASS_POSITION,STATE_CHANGE");

        assertEquals(expectedOptions, configOptions);
    }

    @Test
    void shouldParseAgentArgsThatEndWithExtraSeparators()
    {
        final EnumMap<ConfigOption, String> expectedOptions = new EnumMap<>(ConfigOption.class);
        expectedOptions.put(ENABLED_ARCHIVE_EVENT_CODES, "REPLICATION_SESSION_STATE_CHANGE,CMD_IN_START_RECORDING");

        final EnumMap<ConfigOption, String> configOptions =
            parseAgentArgs("ENABLED_ARCHIVE_EVENT_CODES=REPLICATION_SESSION_STATE_CHANGE,CMD_IN_START_RECORDING||||");

        assertEquals(expectedOptions, configOptions);
    }

    @Test
    void shouldThrowAnIllegalArgumentExceptionUponUnknownConfigOptionNameInTheAgentArgs()
    {
        final IllegalArgumentException exception =
            assertThrows(IllegalArgumentException.class, () -> parseAgentArgs("x=5"));
        assertEquals("No enum constant io.aeron.agent.ConfigOption.x", exception.getMessage());
    }
}
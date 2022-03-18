/*
 * Copyright 2014-2022 Real Logic Limited.
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

import org.agrona.Strings;
import org.agrona.concurrent.Agent;

import java.util.EnumMap;
import java.util.Map;

/**
 * A set of configuration options.
 */
enum ConfigOption
{
    /**
     * Event Buffer log file name system property. If not set then output will default to {@link System#out}.
     */
    LOG_FILENAME("aeron.event.log.filename"),

    /**
     * Event reader {@link Agent} which consumes the {@link EventConfiguration#EVENT_RING_BUFFER} to output log events.
     */
    READER_CLASSNAME("aeron.event.log.reader.classname"),

    /**
     * Driver Event tags system property. This is either:
     * <ul>
     * <li>A comma separated list of {@link DriverEventCode}s to enable.</li>
     * <li>"all" which enables all driver events.</li>
     * <li>"admin" which enables all driver events except for {@link DriverEventCode#FRAME_IN},
     * {@link DriverEventCode#FRAME_OUT}, {@link DriverEventCode#NAME_RESOLUTION_NEIGHBOR_ADDED},
     * {@link DriverEventCode#NAME_RESOLUTION_NEIGHBOR_REMOVED}.</li>
     * </ul>
     */
    ENABLED_DRIVER_EVENT_CODES("aeron.event.log"),

    /**
     * Disabled Driver Event tags system property. Follows the format specified for
     * {@link #ENABLED_DRIVER_EVENT_CODES}. This property will disable any codes in the set
     * specified there. Defined on its own has no effect.
     */
    DISABLED_DRIVER_EVENT_CODES("aeron.event.log.disable"),

    /**
     * Archive Event tags system property. This is either:
     * <ul>
     * <li>A comma separated list of {@link ArchiveEventCode}s to enable.</li>
     * <li>"all" which enables all the codes.</li>
     * </ul>
     */
    ENABLED_ARCHIVE_EVENT_CODES("aeron.event.archive.log"),

    /**
     * Disabled Archive Event tags system property. Follows the format specified for
     * {@link #ENABLED_ARCHIVE_EVENT_CODES}. This property will disable any codes in the
     * set specified there. Defined on its own has no effect.
     */
    DISABLED_ARCHIVE_EVENT_CODES("aeron.event.archive.log.disable"),

    /**
     * Cluster Event tags system property. This is either:
     * <ul>
     * <li>A comma separated list of {@link ClusterEventCode}s to enable.</li>
     * <li>"all" which enables all the codes.</li>
     * <li>"admin" which enables all driver events except for {@link ClusterEventCode#COMMIT_POSITION},
     * {@link ClusterEventCode#APPEND_POSITION} and {@link ClusterEventCode#CANVASS_POSITION}.</li>
     * </ul>
     */
    ENABLED_CLUSTER_EVENT_CODES("aeron.event.cluster.log"),

    /**
     * Disabled Cluster Event tags system property name. Follows the format specified for
     * {@link #ENABLED_CLUSTER_EVENT_CODES}. This property will disable any codes in the
     * set specified there. Defined on its own has no effect.
     */
    DISABLED_CLUSTER_EVENT_CODES("aeron.event.cluster.log.disable");

    static final String START_COMMAND = "start";
    static final String STOP_COMMAND = "stop";

    private static final char VALUE_SEPARATOR = '=';
    private static final char OPTION_SEPARATOR = '|';
    private static final ConfigOption[] OPTIONS = values();

    private final String propertyName;

    ConfigOption(final String propertyName)
    {
        this.propertyName = propertyName;
    }

    String propertyName()
    {
        return propertyName;
    }

    static EnumMap<ConfigOption, String> fromSystemProperties()
    {
        final EnumMap<ConfigOption, String> values = new EnumMap<>(ConfigOption.class);

        for (final ConfigOption option : OPTIONS)
        {
            final String value = System.getProperty(option.propertyName());
            if (null != value)
            {
                values.put(option, value);
            }
        }

        return values;
    }

    static String buildAgentArgs(final EnumMap<ConfigOption, String> configOptions)
    {
        if (configOptions.isEmpty())
        {
            return null;
        }

        final StringBuilder builder = new StringBuilder();
        for (final Map.Entry<ConfigOption, String> entry : configOptions.entrySet())
        {
            builder.append(entry.getKey())
                .append(VALUE_SEPARATOR)
                .append(entry.getValue())
                .append(OPTION_SEPARATOR);
        }

        if (builder.length() > 0)
        {
            builder.setLength(builder.length() - 1);
        }

        return builder.toString();
    }

    static EnumMap<ConfigOption, String> parseAgentArgs(final String agentArgs)
    {
        if (Strings.isEmpty(agentArgs))
        {
            throw new IllegalArgumentException("cannot parse empty value");
        }

        final EnumMap<ConfigOption, String> values = new EnumMap<>(ConfigOption.class);

        int optionIndex = -1;
        do
        {
            final int valueIndex = agentArgs.indexOf(VALUE_SEPARATOR, optionIndex);
            if (valueIndex <= 0)
            {
                break;
            }

            int nameIndex = -1;
            while (optionIndex < valueIndex)
            {
                nameIndex = optionIndex;
                optionIndex = agentArgs.indexOf(OPTION_SEPARATOR, optionIndex + 1);
                if (optionIndex < 0)
                {
                    break;
                }
            }

            final String optionName = agentArgs.substring(nameIndex + 1, valueIndex);
            final ConfigOption option = valueOf(optionName);
            final String value = agentArgs.substring(
                valueIndex + 1,
                optionIndex > 0 ? optionIndex : agentArgs.length());
            values.put(option, value);
        }
        while (optionIndex > 0);

        return values;
    }
}

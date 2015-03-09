/*
 * Copyright 2014 - 2015 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.aeron.common.event;

import uk.co.real_logic.agrona.concurrent.ringbuffer.RingBufferDescriptor;

import java.util.EnumSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static uk.co.real_logic.aeron.common.event.EventCode.*;

/**
 * Common configuration elements between event loggers and event reader side
 */
public class EventConfiguration
{
    /**
     * Event Buffer length system property name
     */
    public static final String BUFFER_LENGTH_PROPERTY_NAME = "aeron.event.buffer.length";

    /**
     * Event tags system property name. This is either:
     * <ul>
     * <li>A comma separated list of EventCodes to enable</li>
     * <li>"all" which enables all the codes</li>
     * <li>"prod" which enables the codes specified by PRODUCTION_LOGGER_EVENT_CODES</li>
     * </ul>
     */
    public static final String ENABLED_LOGGER_EVENT_CODES_PROPERTY_NAME = "aeron.event.log";

    public static final Set<EventCode> PRODUCTION_LOGGER_EVENT_CODES = EnumSet.of(
        EXCEPTION,
        MALFORMED_FRAME_LENGTH,
        ERROR_DELETING_FILE,
        FRAME_OUT_INCOMPLETE_SEND,
        FLOW_CONTROL_OVERRUN);

    public static final Set<EventCode> ADMIN_ONLY_EVENT_CODES = EnumSet.of(
        EXCEPTION,
        MALFORMED_FRAME_LENGTH,
        CMD_IN_ADD_PUBLICATION,
        CMD_IN_ADD_SUBSCRIPTION,
        CMD_IN_KEEPALIVE_CLIENT,
        CMD_IN_REMOVE_PUBLICATION,
        CMD_IN_REMOVE_SUBSCRIPTION,
        REMOVE_CONNECTION_CLEANUP,
        REMOVE_PUBLICATION_CLEANUP,
        REMOVE_SUBSCRIPTION_CLEANUP,
        CMD_OUT_PUBLICATION_READY,
        CMD_OUT_CONNECTION_READY,
        CMD_OUT_ON_INACTIVE_CONNECTION,
        CMD_OUT_ON_OPERATION_SUCCESS,
        ERROR_DELETING_FILE,
        FRAME_OUT_INCOMPLETE_SEND,
        FLOW_CONTROL_OVERRUN,
        CHANNEL_CREATION);

    public static final Set<EventCode> ALL_LOGGER_EVENT_CODES = EnumSet.allOf(EventCode.class);

    /**
     * Event Buffer default length (in bytes)
     */
    public static final int BUFFER_LENGTH_DEFAULT = 65536;

    /**
     * Maximum length of an event in bytes
     */
    public static final int MAX_EVENT_LENGTH = 2048;

    /**
     * Limit for event reader loop
     */
    public static final int EVENT_READER_FRAME_LIMIT = 10;

    private static final Pattern COMMA = Pattern.compile(",");

    public static long getEnabledEventCodes()
    {
        return makeTagBitSet(getEnabledEventCodes(System.getProperty(ENABLED_LOGGER_EVENT_CODES_PROPERTY_NAME)));
    }

    public static long enabledEventCodes(final String enabledLoggerEventCodes)
    {
        return makeTagBitSet(getEnabledEventCodes(enabledLoggerEventCodes));
    }

    public static int bufferLength()
    {
        return Integer.getInteger(
                EventConfiguration.BUFFER_LENGTH_PROPERTY_NAME,
                EventConfiguration.BUFFER_LENGTH_DEFAULT) + RingBufferDescriptor.TRAILER_LENGTH;
    }

    static long makeTagBitSet(final Set<EventCode> eventCodes)
    {
        return eventCodes.stream()
                         .mapToLong(EventCode::tagBit)
                         .reduce(0L, (acc, x) -> acc | x);
    }

    /**
     * Get the {@link Set} of {@link EventCode}s that are enabled for the logger.
     *
     * @param enabledLoggerEventCodes that can be "all", "prod" or a comma separated list.
     * @return the {@link Set} of {@link EventCode}s that are enabled for the logger.
     */
    static Set<EventCode> getEnabledEventCodes(final String enabledLoggerEventCodes)
    {
        if (enabledLoggerEventCodes == null)
        {
            return PRODUCTION_LOGGER_EVENT_CODES;
        }

        switch (enabledLoggerEventCodes)
        {
            case "all":
                return ALL_LOGGER_EVENT_CODES;

            case "prod":
                return PRODUCTION_LOGGER_EVENT_CODES;

            case "admin":
                return ADMIN_ONLY_EVENT_CODES;

            default:
                return COMMA.splitAsStream(enabledLoggerEventCodes)
                            .map(EventCode::valueOf)
                            .collect(Collectors.toSet());
        }
    }
}

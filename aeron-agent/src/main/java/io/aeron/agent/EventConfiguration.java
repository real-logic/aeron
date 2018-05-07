/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.agent;

import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;

import static io.aeron.agent.EventCode.*;

/**
 * Common configuration elements between event loggers and event reader side
 */
public class EventConfiguration
{
    /**
     * Event Buffer length system property name
     */
    public static final String BUFFER_LENGTH_PROP_NAME = "aeron.event.buffer.length";

    /**
     * Event tags system property name. This is either:
     * <ul>
     * <li>A comma separated list of EventCodes to enable</li>
     * <li>"all" which enables all the codes</li>
     * <li>"admin" which enables the codes specified by {@link #ADMIN_ONLY_EVENT_CODES} which is the admin commands</li>
     * </ul>
     */
    public static final String ENABLED_EVENT_CODES_PROP_NAME = "aeron.event.log";

    public static final Set<EventCode> ADMIN_ONLY_EVENT_CODES = EnumSet.of(
        CMD_IN_ADD_PUBLICATION,
        CMD_IN_ADD_SUBSCRIPTION,
        CMD_IN_KEEPALIVE_CLIENT,
        CMD_IN_REMOVE_PUBLICATION,
        CMD_IN_REMOVE_SUBSCRIPTION,
        CMD_IN_ADD_COUNTER,
        CMD_IN_REMOVE_COUNTER,
        CMD_IN_CLIENT_CLOSE,
        CMD_IN_ADD_RCV_DESTINATION,
        CMD_IN_REMOVE_RCV_DESTINATION,
        REMOVE_IMAGE_CLEANUP,
        REMOVE_PUBLICATION_CLEANUP,
        REMOVE_SUBSCRIPTION_CLEANUP,
        CMD_OUT_PUBLICATION_READY,
        CMD_OUT_AVAILABLE_IMAGE,
        CMD_OUT_ON_UNAVAILABLE_IMAGE,
        CMD_OUT_ON_OPERATION_SUCCESS,
        CMD_OUT_ERROR,
        CMD_OUT_SUBSCRIPTION_READY,
        CMD_OUT_COUNTER_READY,
        CMD_OUT_ON_UNAVAILABLE_COUNTER,
        SEND_CHANNEL_CREATION,
        RECEIVE_CHANNEL_CREATION,
        SEND_CHANNEL_CLOSE,
        RECEIVE_CHANNEL_CLOSE);

    public static final Set<EventCode> ALL_LOGGER_EVENT_CODES = EnumSet.allOf(EventCode.class);

    /**
     * Event Buffer default length (in bytes)
     */
    public static final int BUFFER_LENGTH_DEFAULT = 2 * 1024 * 1024;

    /**
     * Maximum length of an event in bytes
     */
    public static final int MAX_EVENT_LENGTH = 4096;

    /**
     * Limit for event reader loop
     */
    public static final int EVENT_READER_FRAME_LIMIT = 8;

    /**
     * The enabled event codes mask
     */
    public static final long ENABLED_EVENT_CODES;

    /**
     * Ring Buffer to use for logging
     */
    public static final ManyToOneRingBuffer EVENT_RING_BUFFER;

    private static final Pattern COMMA_PATTERN = Pattern.compile(",");

    static
    {
        ENABLED_EVENT_CODES = makeTagBitSet(getEnabledEventCodes(System.getProperty(ENABLED_EVENT_CODES_PROP_NAME)));

        final int bufferLength = SystemUtil.getSizeAsInt(
            EventConfiguration.BUFFER_LENGTH_PROP_NAME, EventConfiguration.BUFFER_LENGTH_DEFAULT) +
            RingBufferDescriptor.TRAILER_LENGTH;

        EVENT_RING_BUFFER = new ManyToOneRingBuffer(new UnsafeBuffer(ByteBuffer.allocateDirect(bufferLength)));
    }

    public static long getEnabledEventCodes()
    {
        return ENABLED_EVENT_CODES;
    }

    static long makeTagBitSet(final Set<EventCode> eventCodes)
    {
        long result = 0;

        for (final EventCode eventCode : eventCodes)
        {
            result |= eventCode.tagBit();
        }

        return result;
    }

    /**
     * Get the {@link Set} of {@link EventCode}s that are enabled for the logger.
     *
     * @param enabledLoggerEventCodes that can be "all", "prod", or a comma separated list of Event Code ids or names.
     * @return the {@link Set} of {@link EventCode}s that are enabled for the logger.
     */
    static Set<EventCode> getEnabledEventCodes(final String enabledLoggerEventCodes)
    {
        if (null == enabledLoggerEventCodes || "".equals(enabledLoggerEventCodes))
        {
            return EnumSet.noneOf(EventCode.class);
        }

        final Set<EventCode> eventCodeSet = new HashSet<>();
        final String[] codeIds = COMMA_PATTERN.split(enabledLoggerEventCodes);

        for (final String codeId : codeIds)
        {
            switch (codeId)
            {
                case "all":
                    eventCodeSet.addAll(ALL_LOGGER_EVENT_CODES);
                    break;

                case "admin":
                    eventCodeSet.addAll(ADMIN_ONLY_EVENT_CODES);
                    break;

                default:
                {
                    EventCode code = null;
                    try
                    {
                        code = EventCode.valueOf(codeId);
                    }
                    catch (final IllegalArgumentException ignore)
                    {
                    }

                    if (null == code)
                    {
                        try
                        {
                            code = EventCode.get(Integer.parseInt(codeId));
                        }
                        catch (final IllegalArgumentException ignore)
                        {
                        }
                    }

                    if (null != code)
                    {
                        eventCodeSet.add(code);
                    }
                    else
                    {
                        System.err.println("Unknown event code: " + codeId);
                    }
                }
            }
        }

        return eventCodeSet;
    }
}

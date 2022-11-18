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
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

import static io.aeron.agent.DriverEventCode.*;
import static java.lang.System.err;
import static java.lang.System.lineSeparator;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.SystemUtil.getSizeAsInt;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

/**
 * Common configuration elements between event loggers and event reader side.
 */
final class EventConfiguration
{
    /**
     * Event buffer length system property name.
     */
    static final String BUFFER_LENGTH_PROP_NAME = "aeron.event.buffer.length";

    /**
     * Event Buffer default length (in bytes).
     */
    static final int BUFFER_LENGTH_DEFAULT = 8 * 1024 * 1024;

    /**
     * Maximum length of an event in bytes.
     */
    static final int MAX_EVENT_LENGTH = 4096 - lineSeparator().length();

    /**
     * Iteration limit for event reader loop.
     */
    static final int EVENT_READER_FRAME_LIMIT = 20;

    /**
     * Ring Buffer to use for logging that will be read by {@link ConfigOption#READER_CLASSNAME}.
     */
    static final ManyToOneRingBuffer EVENT_RING_BUFFER;

    static final EnumSet<DriverEventCode> DRIVER_EVENT_CODES = EnumSet.noneOf(DriverEventCode.class);
    static final EnumSet<ArchiveEventCode> ARCHIVE_EVENT_CODES = EnumSet.noneOf(ArchiveEventCode.class);
    static final EnumSet<ClusterEventCode> CLUSTER_EVENT_CODES = EnumSet.noneOf(ClusterEventCode.class);

    static final Object2ObjectHashMap<String, EnumSet<DriverEventCode>> SPECIAL_DRIVER_EVENTS =
        new Object2ObjectHashMap<>();
    static final Object2ObjectHashMap<String, EnumSet<ArchiveEventCode>> SPECIAL_ARCHIVE_EVENTS =
        new Object2ObjectHashMap<>();
    static final Object2ObjectHashMap<String, EnumSet<ClusterEventCode>> SPECIAL_CLUSTER_EVENTS =
        new Object2ObjectHashMap<>();

    static
    {
        SPECIAL_DRIVER_EVENTS.put("all", EnumSet.allOf(DriverEventCode.class));
        SPECIAL_DRIVER_EVENTS.put("admin", EnumSet.complementOf(EnumSet.of(
            FRAME_IN,
            FRAME_OUT,
            NAME_RESOLUTION_NEIGHBOR_ADDED,
            NAME_RESOLUTION_NEIGHBOR_REMOVED)));

        SPECIAL_ARCHIVE_EVENTS.put("all", EnumSet.allOf(ArchiveEventCode.class));

        SPECIAL_CLUSTER_EVENTS.put("all", EnumSet.allOf(ClusterEventCode.class));

        EVENT_RING_BUFFER = new ManyToOneRingBuffer(new UnsafeBuffer(allocateDirectAligned(
            getSizeAsInt(BUFFER_LENGTH_PROP_NAME, BUFFER_LENGTH_DEFAULT) + TRAILER_LENGTH, CACHE_LINE_LENGTH)));
    }

    private EventConfiguration()
    {
    }

    static void init(
        final String enabledDriverEvents,
        final String disabledDriverEvents,
        final String enabledArchiveEvents,
        final String disabledArchiveEvents,
        final String enabledClusterEvents,
        final String disabledClusterEvents)
    {
        DRIVER_EVENT_CODES.clear();
        DRIVER_EVENT_CODES.addAll(getDriverEventCodes(enabledDriverEvents));
        DRIVER_EVENT_CODES.removeAll(getDriverEventCodes(disabledDriverEvents));

        ARCHIVE_EVENT_CODES.clear();
        ARCHIVE_EVENT_CODES.addAll(getArchiveEventCodes(enabledArchiveEvents));
        ARCHIVE_EVENT_CODES.removeAll(getArchiveEventCodes(disabledArchiveEvents));

        CLUSTER_EVENT_CODES.clear();
        CLUSTER_EVENT_CODES.addAll(getClusterEventCodes(enabledClusterEvents));
        CLUSTER_EVENT_CODES.removeAll(getClusterEventCodes(disabledClusterEvents));
    }

    /**
     * Reset configuration back to clear state.
     */
    static void reset()
    {
        DRIVER_EVENT_CODES.clear();
        ARCHIVE_EVENT_CODES.clear();
        CLUSTER_EVENT_CODES.clear();
        EVENT_RING_BUFFER.unblock();
    }

    /**
     * Get the {@link Set} of {@link ArchiveEventCode}s that are enabled for the logger.
     *
     * @param enabledEventCodes that can be "all" or a comma separated list of Event Code ids or names.
     * @return the {@link Set} of {@link ArchiveEventCode}s that are enabled for the logger.
     */
    static EnumSet<ArchiveEventCode> getArchiveEventCodes(final String enabledEventCodes)
    {
        return parseEventCodes(
            ArchiveEventCode.class,
            enabledEventCodes,
            SPECIAL_ARCHIVE_EVENTS,
            ArchiveEventCode::get,
            ArchiveEventCode::valueOf);
    }

    /**
     * Get the {@link Set} of {@link ClusterEventCode}s that are enabled for the logger.
     *
     * @param enabledEventCodes that can be "all" or a comma separated list of Event Code ids or names.
     * @return the {@link Set} of {@link ClusterEventCode}s that are enabled for the logger.
     */
    static EnumSet<ClusterEventCode> getClusterEventCodes(final String enabledEventCodes)
    {
        return parseEventCodes(
            ClusterEventCode.class,
            enabledEventCodes,
            SPECIAL_CLUSTER_EVENTS,
            ClusterEventCode::get,
            ClusterEventCode::valueOf);
    }

    /**
     * Get the {@link Set} of {@link DriverEventCode}s that are enabled for the logger.
     *
     * @param enabledEventCodes that can be "all", "admin", or a comma separated list of Event Code ids or names.
     * @return the {@link Set} of {@link DriverEventCode}s that are enabled for the logger.
     */
    static EnumSet<DriverEventCode> getDriverEventCodes(final String enabledEventCodes)
    {
        return parseEventCodes(
            DriverEventCode.class,
            enabledEventCodes,
            SPECIAL_DRIVER_EVENTS,
            DriverEventCode::get,
            DriverEventCode::valueOf);
    }

    static <E extends Enum<E>> EnumSet<E> parseEventCodes(
        final Class<E> eventCodeType,
        final String eventCodes,
        final Map<String, EnumSet<E>> specialEvents,
        final IntFunction<E> eventCodeById,
        final Function<String, E> eventCodeByName)
    {
        if (Strings.isEmpty(eventCodes))
        {
            return EnumSet.noneOf(eventCodeType);
        }

        final EnumSet<E> eventCodeSet = EnumSet.noneOf(eventCodeType);
        final String[] codeIds = eventCodes.split(",");

        for (final String codeId : codeIds)
        {
            final EnumSet<E> specialCodes = specialEvents.get(codeId);
            if (null != specialCodes)
            {
                eventCodeSet.addAll(specialCodes);
            }
            else
            {
                E code = null;
                try
                {
                    code = eventCodeByName.apply(codeId);
                }
                catch (final IllegalArgumentException ignore)
                {
                }

                if (null == code)
                {
                    try
                    {
                        code = eventCodeById.apply(Integer.parseInt(codeId));
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
                    err.println("unknown event code: " + codeId);
                }
            }
        }

        return eventCodeSet;
    }
}

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

import org.agrona.Strings;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.Function;

import static io.aeron.agent.DriverEventCode.*;
import static java.lang.System.*;
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
    public static final String BUFFER_LENGTH_PROP_NAME = "aeron.event.buffer.length";

    /**
     * Driver Event tags system property name. This is either:
     * <ul>
     * <li>A comma separated list of {@link DriverEventCode}s to enable</li>
     * <li>"all" which enables all the codes</li>
     * <li>"admin" which enables the codes specified by {@link #ADMIN_ONLY_EVENT_CODES} which is the admin commands</li>
     * </ul>
     */
    public static final String ENABLED_EVENT_CODES_PROP_NAME = "aeron.event.log";

    /**
     * Disabled Driver Event tags system property name. Follows the format specified for
     * {@link EventConfiguration#ENABLED_EVENT_CODES_PROP_NAME}. This property will disable any codes in the set
     * specified there. Defined on its own has no effect.
     */
    public static final String DISABLED_EVENT_CODES_PROP_NAME = "aeron.event.log.disable";

    /**
     * Archive Event tags system property name. This is either:
     * <ul>
     * <li>A comma separated list of {@link ArchiveEventCode}s to enable</li>
     * <li>"all" which enables all the codes</li>
     * </ul>
     */
    public static final String ENABLED_ARCHIVE_EVENT_CODES_PROP_NAME = "aeron.event.archive.log";

    /**
     * Disabled Archive Event tags system property name. Follows the format specified for
     * {@link EventConfiguration#ENABLED_ARCHIVE_EVENT_CODES_PROP_NAME}. This property will disable any codes in the
     * set specified there. Defined on its own has no effect.
     */
    public static final String DISABLED_ARCHIVE_EVENT_CODES_PROP_NAME = "aeron.event.archive.log.disable";

    /**
     * Cluster Event tags system property name. This is either:
     * <ul>
     * <li>A comma separated list of {@link ClusterEventCode}s to enable</li>
     * <li>"all" which enables all the codes</li>
     * </ul>
     */
    public static final String ENABLED_CLUSTER_EVENT_CODES_PROP_NAME = "aeron.event.cluster.log";

    /**
     * Disabled Cluster Event tags system property name. Follows the format specified for
     * {@link EventConfiguration#ENABLED_CLUSTER_EVENT_CODES_PROP_NAME}. This property will disable any codes in the
     * set specified there. Defined on its own has no effect.
     */
    public static final String DISABLED_CLUSTER_EVENT_CODES_PROP_NAME = "aeron.event.cluster.log.disable";

    /**
     * Event codes for admin events within the driver, i.e. does not include frame capture and name resolution
     * events.
     */
    public static final Set<DriverEventCode> ADMIN_ONLY_EVENT_CODES = EnumSet.complementOf(EnumSet.of(
        FRAME_IN,
        FRAME_OUT,
        NAME_RESOLUTION_NEIGHBOR_ADDED,
        NAME_RESOLUTION_NEIGHBOR_REMOVED));

    /**
     * Event Buffer default length (in bytes).
     */
    public static final int BUFFER_LENGTH_DEFAULT = 8 * 1024 * 1024;

    /**
     * Maximum length of an event in bytes.
     */
    public static final int MAX_EVENT_LENGTH = 4096 - lineSeparator().length();

    /**
     * Iteration limit for event reader loop.
     */
    public static final int EVENT_READER_FRAME_LIMIT = 20;

    /**
     * Ring Buffer to use for logging that will be read by {@link EventLogAgent#READER_CLASSNAME_PROP_NAME}.
     */
    public static final ManyToOneRingBuffer EVENT_RING_BUFFER;

    static
    {
        EVENT_RING_BUFFER = new ManyToOneRingBuffer(new UnsafeBuffer(allocateDirectAligned(
            getSizeAsInt(BUFFER_LENGTH_PROP_NAME, BUFFER_LENGTH_DEFAULT) + TRAILER_LENGTH, CACHE_LINE_LENGTH)));
    }

    public static final EnumSet<DriverEventCode> DRIVER_EVENT_CODES = EnumSet.noneOf(DriverEventCode.class);
    public static final EnumSet<ArchiveEventCode> ARCHIVE_EVENT_CODES = EnumSet.noneOf(ArchiveEventCode.class);
    public static final EnumSet<ClusterEventCode> CLUSTER_EVENT_CODES = EnumSet.noneOf(ClusterEventCode.class);

    private EventConfiguration()
    {
    }

    static void init()
    {
        DRIVER_EVENT_CODES.clear();
        DRIVER_EVENT_CODES.addAll(getDriverEventCodes(getProperty(ENABLED_EVENT_CODES_PROP_NAME)));
        DRIVER_EVENT_CODES.removeAll(getDriverEventCodes(getProperty(DISABLED_EVENT_CODES_PROP_NAME)));

        ARCHIVE_EVENT_CODES.clear();
        ARCHIVE_EVENT_CODES.addAll(getArchiveEventCodes(getProperty(ENABLED_ARCHIVE_EVENT_CODES_PROP_NAME)));
        ARCHIVE_EVENT_CODES.removeAll(getArchiveEventCodes(getProperty(DISABLED_ARCHIVE_EVENT_CODES_PROP_NAME)));

        CLUSTER_EVENT_CODES.clear();
        CLUSTER_EVENT_CODES.addAll(getClusterEventCodes(getProperty(ENABLED_CLUSTER_EVENT_CODES_PROP_NAME)));
        CLUSTER_EVENT_CODES.removeAll(getClusterEventCodes(getProperty(DISABLED_CLUSTER_EVENT_CODES_PROP_NAME)));
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
        if (Strings.isEmpty(enabledEventCodes))
        {
            return EnumSet.noneOf(ArchiveEventCode.class);
        }

        final Function<Integer, ArchiveEventCode> eventCodeById = ArchiveEventCode::get;
        final Function<String, ArchiveEventCode> eventCodeByName = ArchiveEventCode::valueOf;

        return parseEventCodes(ArchiveEventCode.class, enabledEventCodes, eventCodeById, eventCodeByName);
    }

    /**
     * Get the {@link Set} of {@link ClusterEventCode}s that are enabled for the logger.
     *
     * @param enabledEventCodes that can be "all" or a comma separated list of Event Code ids or names.
     * @return the {@link Set} of {@link ClusterEventCode}s that are enabled for the logger.
     */
    static Set<ClusterEventCode> getClusterEventCodes(final String enabledEventCodes)
    {
        if (Strings.isEmpty(enabledEventCodes))
        {
            return EnumSet.noneOf(ClusterEventCode.class);
        }

        final Function<Integer, ClusterEventCode> eventCodeById = ClusterEventCode::get;
        final Function<String, ClusterEventCode> eventCodeByName = ClusterEventCode::valueOf;

        return parseEventCodes(ClusterEventCode.class, enabledEventCodes, eventCodeById, eventCodeByName);
    }

    /**
     * Get the {@link Set} of {@link DriverEventCode}s that are enabled for the logger.
     *
     * @param enabledEventCodes that can be "all", "admin", or a comma separated list of Event Code ids or names.
     * @return the {@link Set} of {@link DriverEventCode}s that are enabled for the logger.
     */
    static EnumSet<DriverEventCode> getDriverEventCodes(final String enabledEventCodes)
    {
        if (Strings.isEmpty(enabledEventCodes))
        {
            return EnumSet.noneOf(DriverEventCode.class);
        }

        final EnumSet<DriverEventCode> eventCodeSet = EnumSet.noneOf(DriverEventCode.class);
        final String[] codeIds = enabledEventCodes.split(",");

        for (final String codeId : codeIds)
        {
            switch (codeId)
            {
                case "all":
                    return EnumSet.allOf(DriverEventCode.class);

                case "admin":
                    eventCodeSet.addAll(ADMIN_ONLY_EVENT_CODES);
                    break;

                default:
                {
                    DriverEventCode code = null;
                    try
                    {
                        code = DriverEventCode.valueOf(codeId);
                    }
                    catch (final IllegalArgumentException ignore)
                    {
                    }

                    if (null == code)
                    {
                        try
                        {
                            code = DriverEventCode.get(Integer.parseInt(codeId));
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
        }

        return eventCodeSet;
    }

    private static <E extends Enum<E>> EnumSet<E> parseEventCodes(
        final Class<E> eventCodeType,
        final String enabledEventCodes,
        final Function<Integer, E> eventCodeById,
        final Function<String, E> eventCodeByName)
    {
        final EnumSet<E> eventCodeSet = EnumSet.noneOf(eventCodeType);
        final String[] codeIds = enabledEventCodes.split(",");

        for (final String codeId : codeIds)
        {
            if ("all".equals(codeId))
            {
                return EnumSet.allOf(eventCodeType);
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

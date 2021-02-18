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

import org.agrona.MutableDirectBuffer;

import java.util.Arrays;

import static io.aeron.agent.DriverEventDissector.*;

/**
 * Events and codecs for encoding/decoding events recorded to the {@link EventConfiguration#EVENT_RING_BUFFER}.
 */
public enum DriverEventCode implements EventCode
{
    FRAME_IN(1, DriverEventDissector::dissectFrame),
    FRAME_OUT(2, DriverEventDissector::dissectFrame),
    CMD_IN_ADD_PUBLICATION(3, DriverEventDissector::dissectCommand),
    CMD_IN_REMOVE_PUBLICATION(4, DriverEventDissector::dissectCommand),
    CMD_IN_ADD_SUBSCRIPTION(5, DriverEventDissector::dissectCommand),

    CMD_IN_REMOVE_SUBSCRIPTION(6, DriverEventDissector::dissectCommand),
    CMD_OUT_PUBLICATION_READY(7, DriverEventDissector::dissectCommand),
    CMD_OUT_AVAILABLE_IMAGE(8, DriverEventDissector::dissectCommand),

    CMD_OUT_ON_OPERATION_SUCCESS(12, DriverEventDissector::dissectCommand),
    CMD_IN_KEEPALIVE_CLIENT(13, DriverEventDissector::dissectCommand),
    REMOVE_PUBLICATION_CLEANUP(14,
        (code, buffer, offset, builder) -> dissectRemovePublicationCleanup(buffer, offset, builder)),
    REMOVE_SUBSCRIPTION_CLEANUP(15,
        (code, buffer, offset, builder) -> dissectRemoveSubscriptionCleanup(buffer, offset, builder)),

    REMOVE_IMAGE_CLEANUP(16,
        (code, buffer, offset, builder) -> dissectRemoveImageCleanup(buffer, offset, builder)),
    CMD_OUT_ON_UNAVAILABLE_IMAGE(17, DriverEventDissector::dissectCommand),

    SEND_CHANNEL_CREATION(23, DriverEventDissector::dissectString),
    RECEIVE_CHANNEL_CREATION(24, DriverEventDissector::dissectString),
    SEND_CHANNEL_CLOSE(25, DriverEventDissector::dissectString),
    RECEIVE_CHANNEL_CLOSE(26, DriverEventDissector::dissectString),

    CMD_IN_ADD_DESTINATION(30, DriverEventDissector::dissectCommand),
    CMD_IN_REMOVE_DESTINATION(31, DriverEventDissector::dissectCommand),
    CMD_IN_ADD_EXCLUSIVE_PUBLICATION(32, DriverEventDissector::dissectCommand),
    CMD_OUT_EXCLUSIVE_PUBLICATION_READY(33, DriverEventDissector::dissectCommand),

    CMD_OUT_ERROR(34, DriverEventDissector::dissectCommand),

    CMD_IN_ADD_COUNTER(35, DriverEventDissector::dissectCommand),
    CMD_IN_REMOVE_COUNTER(36, DriverEventDissector::dissectCommand),
    CMD_OUT_SUBSCRIPTION_READY(37, DriverEventDissector::dissectCommand),
    CMD_OUT_COUNTER_READY(38, DriverEventDissector::dissectCommand),
    CMD_OUT_ON_UNAVAILABLE_COUNTER(39, DriverEventDissector::dissectCommand),

    CMD_IN_CLIENT_CLOSE(40, DriverEventDissector::dissectCommand),

    CMD_IN_ADD_RCV_DESTINATION(41, DriverEventDissector::dissectCommand),
    CMD_IN_REMOVE_RCV_DESTINATION(42, DriverEventDissector::dissectCommand),

    CMD_OUT_ON_CLIENT_TIMEOUT(43, DriverEventDissector::dissectCommand),
    CMD_IN_TERMINATE_DRIVER(44, DriverEventDissector::dissectCommand),

    UNTETHERED_SUBSCRIPTION_STATE_CHANGE(45,
        (code, buffer, offset, builder) -> dissectUntetheredSubscriptionStateChange(buffer, offset, builder)),

    NAME_RESOLUTION_NEIGHBOR_ADDED(46, DriverEventDissector::dissectAddress),
    NAME_RESOLUTION_NEIGHBOR_REMOVED(47, DriverEventDissector::dissectAddress);

    static final int EVENT_CODE_TYPE = EventCodeType.DRIVER.getTypeCode();

    private static final DriverEventCode[] EVENT_CODE_BY_ID;

    private final int id;
    private final DissectFunction<DriverEventCode> dissector;

    static
    {
        final DriverEventCode[] codes = DriverEventCode.values();
        final int maxId = Arrays.stream(codes).mapToInt(DriverEventCode::id).max().orElse(0);
        EVENT_CODE_BY_ID = new DriverEventCode[maxId + 1];

        for (final DriverEventCode code : codes)
        {
            final int id = code.id();
            if (null != EVENT_CODE_BY_ID[id])
            {
                throw new IllegalArgumentException("id already in use: " + id);
            }

            EVENT_CODE_BY_ID[id] = code;
        }
    }

    DriverEventCode(final int id, final DissectFunction<DriverEventCode> dissector)
    {
        this.id = id;
        this.dissector = dissector;
    }

    /**
     * {@inheritDoc}
     */
    public int id()
    {
        return id;
    }

    static DriverEventCode get(final int id)
    {
        if (id < 0 || id >= EVENT_CODE_BY_ID.length)
        {
            throw new IllegalArgumentException("no DriverEventCode for id: " + id);
        }

        final DriverEventCode code = EVENT_CODE_BY_ID[id];

        if (null == code)
        {
            throw new IllegalArgumentException("no DriverEventCode for id: " + id);
        }

        return code;
    }

    /**
     * Decode an event serialised in a buffer to a provided {@link StringBuilder}.
     *
     * @param buffer  containing the encoded event.
     * @param offset  offset at which the event begins.
     * @param builder to write the decoded event to.
     */
    public void decode(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        dissector.dissect(this, buffer, offset, builder);
    }
}

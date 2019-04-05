/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import org.agrona.MutableDirectBuffer;

/**
 * Events and codecs for encoding/decoding events recorded to the {@link EventConfiguration#EVENT_RING_BUFFER}.
 */
public enum DriverEventCode implements EventCode
{
    FRAME_IN(1, DriverEventDissector::dissectAsFrame),
    FRAME_OUT(2, DriverEventDissector::dissectAsFrame),
    CMD_IN_ADD_PUBLICATION(3, DriverEventDissector::dissectAsCommand),
    CMD_IN_REMOVE_PUBLICATION(4, DriverEventDissector::dissectAsCommand),
    CMD_IN_ADD_SUBSCRIPTION(5, DriverEventDissector::dissectAsCommand),

    CMD_IN_REMOVE_SUBSCRIPTION(6, DriverEventDissector::dissectAsCommand),
    CMD_OUT_PUBLICATION_READY(7, DriverEventDissector::dissectAsCommand),
    CMD_OUT_AVAILABLE_IMAGE(8, DriverEventDissector::dissectAsCommand),
    INVOCATION(9, DriverEventDissector::dissectAsInvocation),

    CMD_OUT_ON_OPERATION_SUCCESS(12, DriverEventDissector::dissectAsCommand),
    CMD_IN_KEEPALIVE_CLIENT(13, DriverEventDissector::dissectAsCommand),
    REMOVE_PUBLICATION_CLEANUP(14, DriverEventDissector::dissectAsString),
    REMOVE_SUBSCRIPTION_CLEANUP(15, DriverEventDissector::dissectAsString),

    REMOVE_IMAGE_CLEANUP(16, DriverEventDissector::dissectAsString),
    CMD_OUT_ON_UNAVAILABLE_IMAGE(17, DriverEventDissector::dissectAsCommand),

    SEND_CHANNEL_CREATION(23, DriverEventDissector::dissectAsString),
    RECEIVE_CHANNEL_CREATION(24, DriverEventDissector::dissectAsString),
    SEND_CHANNEL_CLOSE(25, DriverEventDissector::dissectAsString),
    RECEIVE_CHANNEL_CLOSE(26, DriverEventDissector::dissectAsString),

    CMD_IN_ADD_DESTINATION(30, DriverEventDissector::dissectAsCommand),
    CMD_IN_REMOVE_DESTINATION(31, DriverEventDissector::dissectAsCommand),
    CMD_IN_ADD_EXCLUSIVE_PUBLICATION(32, DriverEventDissector::dissectAsCommand),
    CMD_OUT_EXCLUSIVE_PUBLICATION_READY(33, DriverEventDissector::dissectAsCommand),

    CMD_OUT_ERROR(34, DriverEventDissector::dissectAsCommand),

    CMD_IN_ADD_COUNTER(35, DriverEventDissector::dissectAsCommand),
    CMD_IN_REMOVE_COUNTER(36, DriverEventDissector::dissectAsCommand),
    CMD_OUT_SUBSCRIPTION_READY(37, DriverEventDissector::dissectAsCommand),
    CMD_OUT_COUNTER_READY(38, DriverEventDissector::dissectAsCommand),
    CMD_OUT_ON_UNAVAILABLE_COUNTER(39, DriverEventDissector::dissectAsCommand),

    CMD_IN_CLIENT_CLOSE(40, DriverEventDissector::dissectAsCommand),

    CMD_IN_ADD_RCV_DESTINATION(41, DriverEventDissector::dissectAsCommand),
    CMD_IN_REMOVE_RCV_DESTINATION(42, DriverEventDissector::dissectAsCommand),

    CMD_OUT_ON_CLIENT_TIMEOUT(43, DriverEventDissector::dissectAsCommand),
    CMD_IN_TERMINATE_DRIVER(44, DriverEventDissector::dissectAsCommand);

    static final int EVENT_CODE_TYPE = EventCodeType.DRIVER.getTypeCode();

    private static final int MAX_ID = 63;
    private static final DriverEventCode[] EVENT_CODE_BY_ID = new DriverEventCode[MAX_ID];

    private final long tagBit;
    private final int id;
    private final DissectFunction<DriverEventCode> dissector;

    static
    {
        for (final DriverEventCode code : DriverEventCode.values())
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
        this.tagBit = 1L << id;
        this.dissector = dissector;
    }

    /**
     * {@inheritDoc}
     */
    public int id()
    {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    public long tagBit()
    {
        return tagBit;
    }

    /**
     * {@inheritDoc}
     */
    public EventCodeType eventCodeType()
    {
        return EventCodeType.DRIVER;
    }

    public static DriverEventCode get(final int id)
    {
        if (id < 0 || id > MAX_ID)
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

    public static boolean isEnabled(final DriverEventCode code, final long mask)
    {
        return (mask & code.tagBit()) == code.tagBit();
    }

    public void decode(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        dissector.dissect(this, buffer, offset, builder);
    }
}

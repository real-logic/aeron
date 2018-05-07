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

import org.agrona.MutableDirectBuffer;

/**
 * Event types and association for encoding/decoding.
 */
public enum EventCode
{
    FRAME_IN(1, EventDissector::dissectAsFrame),
    FRAME_OUT(2, EventDissector::dissectAsFrame),
    CMD_IN_ADD_PUBLICATION(3, EventDissector::dissectAsCommand),
    CMD_IN_REMOVE_PUBLICATION(4, EventDissector::dissectAsCommand),
    CMD_IN_ADD_SUBSCRIPTION(5, EventDissector::dissectAsCommand),

    CMD_IN_REMOVE_SUBSCRIPTION(6, EventDissector::dissectAsCommand),
    CMD_OUT_PUBLICATION_READY(7, EventDissector::dissectAsCommand),
    CMD_OUT_AVAILABLE_IMAGE(8, EventDissector::dissectAsCommand),
    INVOCATION(9, EventDissector::dissectAsInvocation),

    CMD_OUT_ON_OPERATION_SUCCESS(12, EventDissector::dissectAsCommand),
    CMD_IN_KEEPALIVE_CLIENT(13, EventDissector::dissectAsCommand),
    REMOVE_PUBLICATION_CLEANUP(14, EventDissector::dissectAsString),
    REMOVE_SUBSCRIPTION_CLEANUP(15, EventDissector::dissectAsString),

    REMOVE_IMAGE_CLEANUP(16, EventDissector::dissectAsString),
    CMD_OUT_ON_UNAVAILABLE_IMAGE(17, EventDissector::dissectAsCommand),

    SEND_CHANNEL_CREATION(23, EventDissector::dissectAsString),
    RECEIVE_CHANNEL_CREATION(24, EventDissector::dissectAsString),
    SEND_CHANNEL_CLOSE(25, EventDissector::dissectAsString),
    RECEIVE_CHANNEL_CLOSE(26, EventDissector::dissectAsString),

    CMD_IN_ADD_DESTINATION(30, EventDissector::dissectAsCommand),
    CMD_IN_REMOVE_DESTINATION(31, EventDissector::dissectAsCommand),
    CMD_IN_ADD_EXCLUSIVE_PUBLICATION(32, EventDissector::dissectAsCommand),
    CMD_OUT_EXCLUSIVE_PUBLICATION_READY(33, EventDissector::dissectAsCommand),

    CMD_OUT_ERROR(34, EventDissector::dissectAsCommand),

    CMD_IN_ADD_COUNTER(35, EventDissector::dissectAsCommand),
    CMD_IN_REMOVE_COUNTER(36, EventDissector::dissectAsCommand),
    CMD_OUT_SUBSCRIPTION_READY(37, EventDissector::dissectAsCommand),
    CMD_OUT_COUNTER_READY(38, EventDissector::dissectAsCommand),
    CMD_OUT_ON_UNAVAILABLE_COUNTER(39, EventDissector::dissectAsCommand),

    CMD_IN_CLIENT_CLOSE(40, EventDissector::dissectAsCommand),

    CMD_IN_ADD_RCV_DESTINATION(41, EventDissector::dissectAsCommand),
    CMD_IN_REMOVE_RCV_DESTINATION(42, EventDissector::dissectAsCommand);

    private static final int MAX_ID = 63;
    private static final EventCode[] EVENT_CODE_BY_ID = new EventCode[MAX_ID];

    @FunctionalInterface
    private interface DissectFunction
    {
        String dissect(EventCode code, MutableDirectBuffer buffer, int offset);
    }

    private final long tagBit;
    private final int id;
    private final DissectFunction dissector;

    static
    {
        for (final EventCode code : EventCode.values())
        {
            final int id = code.id();
            if (null != EVENT_CODE_BY_ID[id])
            {
                throw new IllegalArgumentException("id already in use: " + id);
            }

            EVENT_CODE_BY_ID[id] = code;
        }
    }

    EventCode(final int id, final DissectFunction dissector)
    {
        this.id = id;
        this.tagBit = 1L << id;
        this.dissector = dissector;
    }

    public int id()
    {
        return id;
    }

    /**
     * Get the event code's tag bit. Each tag bit is a unique identifier for the event code used
     * when checking that the event code is enabled or not. Each EventCode has a unique tag bit.
     *
     * @return the tag bit
     */
    public long tagBit()
    {
        return tagBit;
    }

    public static EventCode get(final int id)
    {
        if (id < 0 || id > MAX_ID)
        {
            throw new IllegalArgumentException("no EventCode for id: " + id);
        }

        final EventCode code = EVENT_CODE_BY_ID[id];

        if (null == code)
        {
            throw new IllegalArgumentException("no EventCode for id: " + id);
        }

        return code;
    }

    public static boolean isEnabled(final EventCode code, final long mask)
    {
        return ((mask & code.tagBit()) == code.tagBit());
    }

    public String decode(final MutableDirectBuffer buffer, final int offset)
    {
        return dissector.dissect(this, buffer, offset);
    }
}

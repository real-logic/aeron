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
package uk.co.real_logic.aeron.driver.event;

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

/**
 * Event types and encoding/decoding
 */
public enum EventCode
{
    FRAME_IN(1, EventCodec::dissectAsFrame),
    FRAME_OUT(2, EventCodec::dissectAsFrame),
    CMD_IN_ADD_PUBLICATION(3, EventCodec::dissectAsCommand),
    CMD_IN_REMOVE_PUBLICATION(4, EventCodec::dissectAsCommand),
    CMD_IN_ADD_SUBSCRIPTION(5, EventCodec::dissectAsCommand),

    CMD_IN_REMOVE_SUBSCRIPTION(6, EventCodec::dissectAsCommand),
    CMD_OUT_PUBLICATION_READY(7, EventCodec::dissectAsCommand),
    CMD_OUT_CONNECTION_READY(8, EventCodec::dissectAsCommand),
    INVOCATION(9, EventCodec::dissectAsInvocation),
    EXCEPTION(10, EventCodec::dissectAsException),

    MALFORMED_FRAME_LENGTH(11, EventCodec::dissectAsCommand),
    CMD_OUT_ON_OPERATION_SUCCESS(12, EventCodec::dissectAsCommand),
    CMD_IN_KEEPALIVE_CLIENT(13, EventCodec::dissectAsCommand),
    REMOVE_PUBLICATION_CLEANUP(14, EventCodec::dissectAsString),
    REMOVE_SUBSCRIPTION_CLEANUP(15, EventCodec::dissectAsString),

    REMOVE_CONNECTION_CLEANUP(16, EventCodec::dissectAsString),
    CMD_OUT_ON_INACTIVE_CONNECTION(17, EventCodec::dissectAsCommand),
    FRAME_IN_DROPPED(18, EventCodec::dissectAsFrame),
    ERROR_DELETING_FILE(19, EventCodec::dissectAsString),

    INVALID_VERSION(22, EventCodec::dissectAsCommand),

    CHANNEL_CREATION(23, EventCodec::dissectAsString);

    private static final Int2ObjectHashMap<EventCode> EVENT_CODE_BY_ID_MAP = new Int2ObjectHashMap<>();

    @FunctionalInterface
    private interface DissectFunction
    {
        String dissect(final EventCode code, final MutableDirectBuffer buffer, final int offset, final int length);
    }

    private long tagBit;
    private final int id;
    private final DissectFunction dissector;

    static
    {
        for (final EventCode e : EventCode.values())
        {
            EVENT_CODE_BY_ID_MAP.put(e.id(), e);
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
     * returns the event code's tag bit. Each tag bit is a unique identifier for the event code used
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
        final EventCode code = EVENT_CODE_BY_ID_MAP.get(id);

        if (null == code)
        {
            throw new IllegalArgumentException("No EventCode for ID: " + id);
        }

        return code;
    }

    public String decode(final MutableDirectBuffer buffer, final int offset, final int length)
    {
        return dissector.dissect(this, buffer, offset, length);
    }
}

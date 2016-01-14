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
    FRAME_IN(1, EventDissector::dissectAsFrame),
    FRAME_OUT(2, EventDissector::dissectAsFrame),
    CMD_IN_ADD_PUBLICATION(3, EventDissector::dissectAsCommand),
    CMD_IN_REMOVE_PUBLICATION(4, EventDissector::dissectAsCommand),
    CMD_IN_ADD_SUBSCRIPTION(5, EventDissector::dissectAsCommand),

    CMD_IN_REMOVE_SUBSCRIPTION(6, EventDissector::dissectAsCommand),
    CMD_OUT_PUBLICATION_READY(7, EventDissector::dissectAsCommand),
    CMD_OUT_AVAILABLE_IMAGE(8, EventDissector::dissectAsCommand),
    INVOCATION(9, EventDissector::dissectAsInvocation),
    EXCEPTION(10, EventDissector::dissectAsException),

    MALFORMED_FRAME_LENGTH(11, EventDissector::dissectAsCommand),
    CMD_OUT_ON_OPERATION_SUCCESS(12, EventDissector::dissectAsCommand),
    CMD_IN_KEEPALIVE_CLIENT(13, EventDissector::dissectAsCommand),
    REMOVE_PUBLICATION_CLEANUP(14, EventDissector::dissectAsString),
    REMOVE_SUBSCRIPTION_CLEANUP(15, EventDissector::dissectAsString),

    REMOVE_IMAGE_CLEANUP(16, EventDissector::dissectAsString),
    CMD_OUT_ON_UNAVAILABLE_IMAGE(17, EventDissector::dissectAsCommand),
    FRAME_IN_DROPPED(18, EventDissector::dissectAsFrame),
    ERROR_DELETING_FILE(19, EventDissector::dissectAsString),

    INVALID_VERSION(22, EventDissector::dissectAsCommand),

    CHANNEL_CREATION(23, EventDissector::dissectAsString);

    private static final Int2ObjectHashMap<EventCode> EVENT_CODE_BY_ID_MAP = new Int2ObjectHashMap<>();

    @FunctionalInterface
    private interface DissectFunction
    {
        String dissect(final EventCode code, final MutableDirectBuffer buffer, final int offset);
    }

    private final long tagBit;
    private final int id;
    private final DissectFunction dissector;

    static
    {
        for (final EventCode code : EventCode.values())
        {
            EVENT_CODE_BY_ID_MAP.put(code.id(), code);
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
        final EventCode code = EVENT_CODE_BY_ID_MAP.get(id);

        if (null == code)
        {
            throw new IllegalArgumentException("No EventCode for ID: " + id);
        }

        return code;
    }

    public String decode(final MutableDirectBuffer buffer, final int offset)
    {
        return dissector.dissect(this, buffer, offset);
    }
}

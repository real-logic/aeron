/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.util.event;

import uk.co.real_logic.aeron.util.collections.Int2ObjectHashMap;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

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
    CMD_OUT_NEW_PUBLICATION_BUFFER_NOTIFICATION(7, EventCodec::dissectAsCommand),
    CMD_OUT_NEW_SUBSCRIPTION_BUFFER_NOTIFICATION(8, EventCodec::dissectAsCommand),
    INVOCATION(9, EventCodec::dissectAsInvocation),
    EXCEPTION(10, EventCodec::dissectAsException),
    MALFORMED_FRAME_LENGTH(11, EventCodec::dissectAsCommand),
    UNKNOWN_HEADER_TYPE(12, EventCodec::dissectAsCommand),

    /** Probably means MULTICAST_DEFAULT_INTERFACE_PROP_NAME wasn't set properly */
    COULD_NOT_FIND_INTERFACE(13, EventCodec::dissectAsString),
    COULD_NOT_FIND_FRAME_HANDLER_FOR_NEW_CONNECTED_SUBSCRIPTION(14, EventCodec::dissectAsString),
    ERROR_SENDING_HEARTBEAT_PACKET(15, EventCodec::dissectAsFrame),
    COULD_NOT_SEND_ENTIRE_RETRANSMIT(16, EventCodec::dissectAsFrame);
    
    private final static Int2ObjectHashMap<EventCode> mapOfIdToEventCode = new Int2ObjectHashMap<>();

    @FunctionalInterface
    private interface DissectFunction
    {
        String dissect(final EventCode code, final AtomicBuffer buffer, final int offset, final int length);
    }

    private final int id;
    private final DissectFunction dissector;

    static
    {
        for (final EventCode e : EventCode.values())
        {
            mapOfIdToEventCode.put(e.id(), e);
        }
    }

    EventCode(final int id, final DissectFunction dissector)
    {
        this.id = id;
        this.dissector = dissector;
    }

    public int id()
    {
        return id;
    }

    public static EventCode get(final int id)
    {
        final EventCode code = mapOfIdToEventCode.get(id);

        if (null == code)
        {
            throw new IllegalArgumentException("No EventCode for ID: " + id);
        }

        return code;
    }

    public String decode(final AtomicBuffer buffer, final int offset, final int length)
    {
        return dissector.dissect(this, buffer, offset, length);
    }
}

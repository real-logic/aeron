/*
 * Copyright 2016 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver.stats;

import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.CountersManager;

/**
 * System wide counters for monitoring. These are separate from counters used for position tracking on streams.
 */
public enum SystemCounterDescriptor
{
    BYTES_SENT(0, "Bytes Sent"),
    BYTES_RECEIVED(1, "Bytes received"),
    RECEIVER_PROXY_FAILS(2, "Failed offers to ReceiverProxy"),
    SENDER_PROXY_FAILS(3, "Failed offers to SenderProxy"),
    CONDUCTOR_PROXY_FAILS(4, "Failed offers to DriverConductorProxy"),
    NAK_MESSAGES_SENT(5, "NAKs sent"),
    NAK_MESSAGES_RECEIVED(6, "NAKs received"),
    STATUS_MESSAGES_SENT(7, "Status Messages sent"),
    STATUS_MESSAGES_RECEIVED(8, "Status Messages received"),
    HEARTBEATS_SENT(9, "Heartbeats sent"),
    HEARTBEATS_RECEIVED(10, "Heartbeats received"),
    RETRANSMITS_SENT(11, "Retransmits sent"),
    FLOW_CONTROL_UNDER_RUNS(12, "Flow control under runs"),
    FLOW_CONTROL_OVER_RUNS(13, "Flow control over runs"),
    INVALID_PACKETS(14, "Invalid packets"),
    ERRORS(15, "Errors"),
    DATA_PACKET_SHORT_SENDS(16, "Data Packet short sends"),
    SETUP_MESSAGE_SHORT_SENDS(17, "Setup Message short sends"),
    STATUS_MESSAGE_SHORT_SENDS(18, "Status Message short sends"),
    NAK_MESSAGE_SHORT_SENDS(19, "NAK Message short sends"),
    CLIENT_KEEP_ALIVES(20, "Client keep-alives"),
    SENDER_FLOW_CONTROL_LIMITS(21, "Sender flow control limits applied"),
    UNBLOCKED_PUBLICATIONS(22, "Unblocked Publications"),
    UNBLOCKED_COMMANDS(23, "Unblocked Control Commands");

    /**
     * All system counters have the same type id, i.e. system counters are the same type. Others types can exist.
     */
    public static final int SYSTEM_COUNTER_TYPE_ID = 0;

    private static final Int2ObjectHashMap<SystemCounterDescriptor> DESCRIPTOR_BY_ID_MAP = new Int2ObjectHashMap<>();

    static
    {
        for (final SystemCounterDescriptor descriptor : SystemCounterDescriptor.values())
        {
            if (null != DESCRIPTOR_BY_ID_MAP.put(descriptor.id, descriptor))
            {
                throw new IllegalStateException("Descriptor id already in use: " + descriptor.id);
            }
        }
    }

    /**
     * Get the {@link SystemCounterDescriptor} for a given id.
     *
     * @param id for the descriptor.
     * @return the descriptor if found otherwise null.
     */
    public static SystemCounterDescriptor get(final int id)
    {
        return DESCRIPTOR_BY_ID_MAP.get(id);
    }

    private final int id;
    private final String label;

    SystemCounterDescriptor(final int id, final String label)
    {
        this.id = id;
        this.label = label;
    }

    /**
     * The unique identity for the system counter.
     *
     * @return the unique identity for the system counter.
     */
    public int id()
    {
        return id;
    }

    /**
     * The human readable label to identify a system counter.
     *
     * @return the human readable label to identify a system counter.
     */
    public String label()
    {
        return label;
    }

    /**
     * Create a new counter for the enumerated descriptor.
     *
     * @param countersManager for managing the underlying storage.
     * @return a new counter for the enumerated descriptor.
     */
    public AtomicCounter newCounter(final CountersManager countersManager)
    {
        return countersManager.newCounter(label, SYSTEM_COUNTER_TYPE_ID, (buffer) -> buffer.putInt(0, id));
    }
}

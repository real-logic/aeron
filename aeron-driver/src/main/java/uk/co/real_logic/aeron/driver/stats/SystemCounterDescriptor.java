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

public enum SystemCounterDescriptor
{
    BYTES_SENT(1, "Bytes Sent"),
    BYTES_RECEIVED(2, "Bytes received"),
    RECEIVER_PROXY_FAILS(3, "Failed offers to ReceiverProxy"),
    SENDER_PROXY_FAILS(4, "Failed offers to SenderProxy"),
    CONDUCTOR_PROXY_FAILS(5, "Failed offers to DriverConductorProxy"),
    NAK_MESSAGES_SENT(6, "NAKs sent"),
    NAK_MESSAGES_RECEIVED(7, "NAKs received"),
    STATUS_MESSAGES_SENT(8, "Status Messages sent"),
    STATUS_MESSAGES_RECEIVED(9, "Status Messages received"),
    HEARTBEATS_SENT(10, "Heartbeats sent"),
    HEARTBEATS_RECEIVED(11, "Heartbeats received"),
    RETRANSMITS_SENT(12, "Retransmits sent"),
    FLOW_CONTROL_UNDER_RUNS(13, "Flow control under runs"),
    FLOW_CONTROL_OVER_RUNS(14, "Flow control over runs"),
    INVALID_PACKETS(15, "Invalid packets"),
    ERRORS(16, "Errors"),
    DATA_PACKET_SHORT_SENDS(17, "Data Packet short sends"),
    SETUP_MESSAGE_SHORT_SENDS(18, "Setup Message short sends"),
    STATUS_MESSAGE_SHORT_SENDS(19, "Status Message short sends"),
    NAK_MESSAGE_SHORT_SENDS(20, "NAK Message short sends"),
    CLIENT_KEEP_ALIVES(21, "Client keep-alives"),
    SENDER_FLOW_CONTROL_LIMITS(22, "Sender flow control limits applied"),
    UNBLOCKED_PUBLICATIONS(23, "Unblocked Publications"),
    UNBLOCKED_COMMANDS(24, "Unblocked Control Commands");

    /**
     * All system counters have the same type id.
     */
    public static final int SYSTEM_COUNTER_TYPE_ID = 0;

    private static final Int2ObjectHashMap<SystemCounterDescriptor> DESCRIPTOR_BY_ID_MAP = new Int2ObjectHashMap<>();

    static
    {
        for (final SystemCounterDescriptor descriptor : SystemCounterDescriptor.values())
        {
            DESCRIPTOR_BY_ID_MAP.put(descriptor.id, descriptor);
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

    public AtomicCounter newCounter(final CountersManager countersManager)
    {
        return countersManager.newCounter(label, SYSTEM_COUNTER_TYPE_ID, (buffer) -> buffer.putInt(0, id));
    }
}

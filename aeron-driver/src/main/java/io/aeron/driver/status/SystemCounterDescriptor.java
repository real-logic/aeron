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
package io.aeron.driver.status;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.status.*;

/**
 * System wide counters for monitoring. These are separate from counters used for position tracking on streams.
 */
public enum SystemCounterDescriptor
{
    BYTES_SENT(0, "Bytes sent"),
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
    SHORT_SENDS(16, "Short sends"),
    FREE_FAILS(17, "Failed attempts to free log buffers"),
    SENDER_FLOW_CONTROL_LIMITS(18, "Sender flow control limits applied"),
    UNBLOCKED_PUBLICATIONS(19, "Unblocked Publications"),
    UNBLOCKED_COMMANDS(20, "Unblocked Control Commands"),
    POSSIBLE_TTL_ASYMMETRY(21, "Possible TTL Asymmetry"),
    CONTROLLABLE_IDLE_STRATEGY(22, "ControllableIdleStrategy status"),
    LOSS_GAP_FILLS(23, "Loss gap fills"),
    CLIENT_TIMEOUTS(24, "Client liveness timeouts");

    /**
     * All system counters have the same type id, i.e. system counters are the same type. Other types can exist.
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

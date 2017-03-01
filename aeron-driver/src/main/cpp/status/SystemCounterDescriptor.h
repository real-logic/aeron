/*
 * Copyright 2014-2017 Real Logic Ltd.
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
#ifndef AERON_SYSTEMCOUNTERDESCRIPTOR_H
#define AERON_SYSTEMCOUNTERDESCRIPTOR_H

#include <cstdint>

namespace aeron { namespace driver { namespace status {

using namespace aeron::concurrent;


class SystemCounterDescriptor {

public:
    static const std::int32_t VALUES_SIZE = 25;
    typedef std::array<SystemCounterDescriptor, VALUES_SIZE> values_t;

    static const std::int32_t COUNT = 1;

    static const SystemCounterDescriptor BYTES_SENT;
    static const SystemCounterDescriptor BYTES_RECEIVED;
    static const SystemCounterDescriptor RECEIVER_PROXY_FAILS;
    static const SystemCounterDescriptor SENDER_PROXY_FAILS;
    static const SystemCounterDescriptor CONDUCTOR_PROXY_FAILS;
    static const SystemCounterDescriptor NAK_MESSAGES_SENT;
    static const SystemCounterDescriptor NAK_MESSAGES_RECEIVED;
    static const SystemCounterDescriptor STATUS_MESSAGES_SENT;
    static const SystemCounterDescriptor STATUS_MESSAGES_RECEIVED;
    static const SystemCounterDescriptor HEARTBEATS_SENT;
    static const SystemCounterDescriptor HEARTBEATS_RECEIVED;
    static const SystemCounterDescriptor RETRANSMITS_SENT;
    static const SystemCounterDescriptor FLOW_CONTROL_UNDER_RUNS;
    static const SystemCounterDescriptor FLOW_CONTROL_OVER_RUNS;
    static const SystemCounterDescriptor INVALID_PACKETS;
    static const SystemCounterDescriptor ERRORS;
    static const SystemCounterDescriptor DATA_PACKET_SHORT_SENDS;
    static const SystemCounterDescriptor SETUP_MESSAGE_SHORT_SENDS;
    static const SystemCounterDescriptor STATUS_MESSAGE_SHORT_SENDS;
    static const SystemCounterDescriptor NAK_MESSAGE_SHORT_SENDS;
    static const SystemCounterDescriptor CLIENT_KEEP_ALIVES;
    static const SystemCounterDescriptor SENDER_FLOW_CONTROL_LIMITS;
    static const SystemCounterDescriptor UNBLOCKED_PUBLICATIONS;
    static const SystemCounterDescriptor UNBLOCKED_COMMANDS;
    static const SystemCounterDescriptor POSSIBLE_TTL_ASYMMETRY;

    static const values_t VALUES;

    static inline const SystemCounterDescriptor getById(std::int32_t id)
    {
        return BYTES_SENT;
    }

    AtomicCounter* newCounter(CountersManager& countersManager)
    {
        std::int32_t counterId = countersManager.allocate(m_label, m_id, [&](AtomicBuffer &buffer)
        {
            buffer.putInt32(0, m_id);
        });

        return new AtomicCounter(countersManager.valuesBuffer(), counterId, countersManager);
    }

    inline std::int32_t id() const
    {
        return m_id;
    }

    inline const char* label() const
    {
        return m_label;
    }

private:
    SystemCounterDescriptor(std::int32_t id, const char* label)
    : m_id(id), m_label(label)
    {
    }

    const std::int32_t m_id;
    const char* m_label;
};

const SystemCounterDescriptor SystemCounterDescriptor::BYTES_SENT = SystemCounterDescriptor{0, "Bytes Sent"};
const SystemCounterDescriptor SystemCounterDescriptor::BYTES_RECEIVED = SystemCounterDescriptor(1, "Bytes received");
const SystemCounterDescriptor SystemCounterDescriptor::RECEIVER_PROXY_FAILS = SystemCounterDescriptor(2, "Failed offers to ReceiverProxy");
const SystemCounterDescriptor SystemCounterDescriptor::SENDER_PROXY_FAILS = SystemCounterDescriptor(3, "Failed offers to SenderProxy");
const SystemCounterDescriptor SystemCounterDescriptor::CONDUCTOR_PROXY_FAILS = SystemCounterDescriptor(4, "Failed offers to DriverConductorProxy");
const SystemCounterDescriptor SystemCounterDescriptor::NAK_MESSAGES_SENT = SystemCounterDescriptor(5, "NAKs sent");
const SystemCounterDescriptor SystemCounterDescriptor::NAK_MESSAGES_RECEIVED = SystemCounterDescriptor(6, "NAKs received");
const SystemCounterDescriptor SystemCounterDescriptor::STATUS_MESSAGES_SENT = SystemCounterDescriptor(7, "Status Messages sent");
const SystemCounterDescriptor SystemCounterDescriptor::STATUS_MESSAGES_RECEIVED = SystemCounterDescriptor(8, "Status Messages received");
const SystemCounterDescriptor SystemCounterDescriptor::HEARTBEATS_SENT = SystemCounterDescriptor(9, "Heartbeats sent");
const SystemCounterDescriptor SystemCounterDescriptor::HEARTBEATS_RECEIVED = SystemCounterDescriptor(10, "Heartbeats received");
const SystemCounterDescriptor SystemCounterDescriptor::RETRANSMITS_SENT = SystemCounterDescriptor(11, "Retransmits sent");
const SystemCounterDescriptor SystemCounterDescriptor::FLOW_CONTROL_UNDER_RUNS = SystemCounterDescriptor(12, "Flow control under runs");
const SystemCounterDescriptor SystemCounterDescriptor::FLOW_CONTROL_OVER_RUNS = SystemCounterDescriptor(13, "Flow control over runs");
const SystemCounterDescriptor SystemCounterDescriptor::INVALID_PACKETS = SystemCounterDescriptor(14, "Invalid packets");
const SystemCounterDescriptor SystemCounterDescriptor::ERRORS = SystemCounterDescriptor(15, "Errors");
const SystemCounterDescriptor SystemCounterDescriptor::DATA_PACKET_SHORT_SENDS = SystemCounterDescriptor(16, "Data Packet short sends");
const SystemCounterDescriptor SystemCounterDescriptor::SETUP_MESSAGE_SHORT_SENDS = SystemCounterDescriptor(17, "Setup Message short sends");
const SystemCounterDescriptor SystemCounterDescriptor::STATUS_MESSAGE_SHORT_SENDS = SystemCounterDescriptor(18, "Status Message short sends");
const SystemCounterDescriptor SystemCounterDescriptor::NAK_MESSAGE_SHORT_SENDS = SystemCounterDescriptor(19, "NAK Message short sends");
const SystemCounterDescriptor SystemCounterDescriptor::CLIENT_KEEP_ALIVES = SystemCounterDescriptor(20, "Client keep-alives");
const SystemCounterDescriptor SystemCounterDescriptor::SENDER_FLOW_CONTROL_LIMITS = SystemCounterDescriptor(21, "Sender flow control limits applied");
const SystemCounterDescriptor SystemCounterDescriptor::UNBLOCKED_PUBLICATIONS = SystemCounterDescriptor(22, "Unblocked Publications");
const SystemCounterDescriptor SystemCounterDescriptor::UNBLOCKED_COMMANDS = SystemCounterDescriptor(23, "Unblocked Control Commands");
const SystemCounterDescriptor SystemCounterDescriptor::POSSIBLE_TTL_ASYMMETRY = SystemCounterDescriptor(24, "Possible TTL Asymmetry");

const SystemCounterDescriptor::values_t SystemCounterDescriptor::VALUES = {
    SystemCounterDescriptor::BYTES_SENT,
    SystemCounterDescriptor::BYTES_RECEIVED,
    SystemCounterDescriptor::RECEIVER_PROXY_FAILS,
    SystemCounterDescriptor::SENDER_PROXY_FAILS,
    SystemCounterDescriptor::CONDUCTOR_PROXY_FAILS,
    SystemCounterDescriptor::NAK_MESSAGES_SENT,
    SystemCounterDescriptor::NAK_MESSAGES_RECEIVED,
    SystemCounterDescriptor::STATUS_MESSAGES_SENT,
    SystemCounterDescriptor::STATUS_MESSAGES_RECEIVED,
    SystemCounterDescriptor::HEARTBEATS_SENT,
    SystemCounterDescriptor::HEARTBEATS_RECEIVED,
    SystemCounterDescriptor::RETRANSMITS_SENT,
    SystemCounterDescriptor::FLOW_CONTROL_UNDER_RUNS,
    SystemCounterDescriptor::FLOW_CONTROL_OVER_RUNS,
    SystemCounterDescriptor::INVALID_PACKETS,
    SystemCounterDescriptor::ERRORS,
    SystemCounterDescriptor::DATA_PACKET_SHORT_SENDS,
    SystemCounterDescriptor::SETUP_MESSAGE_SHORT_SENDS,
    SystemCounterDescriptor::STATUS_MESSAGE_SHORT_SENDS,
    SystemCounterDescriptor::NAK_MESSAGE_SHORT_SENDS,
    SystemCounterDescriptor::CLIENT_KEEP_ALIVES,
    SystemCounterDescriptor::SENDER_FLOW_CONTROL_LIMITS,
    SystemCounterDescriptor::UNBLOCKED_PUBLICATIONS,
    SystemCounterDescriptor::UNBLOCKED_COMMANDS,
    SystemCounterDescriptor::POSSIBLE_TTL_ASYMMETRY
};

}}}

#endif

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

#ifndef INCLUDED_AERON_CONCURRENT_BROADCAST_COPY_BROADCAST_RECEIVER__
#define INCLUDED_AERON_CONCURRENT_BROADCAST_COPY_BROADCAST_RECEIVER__

#include <array>
#include <functional>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "BroadcastBufferDescriptor.h"
#include "RecordDescriptor.h"
#include "BroadcastReceiver.h"

namespace aeron { namespace concurrent { namespace broadcast {

typedef std::array<std::uint8_t, 4096> scratch_buffer_t;

/** The data handler function signature */
typedef std::function<void(std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)> handler_t;

class CopyBroadcastReceiver
{
public:
    CopyBroadcastReceiver(BroadcastReceiver& receiver) :
        m_receiver(receiver),
        m_scratchBuffer(&m_scratch[0], m_scratch.size())
    {
        while (m_receiver.receiveNext())
        {
        }
    }

    int receive(const handler_t& handler)
    {
        int messagesReceived = 0;
        const long lastSeenLappedCount = m_receiver.lappedCount();

        if (m_receiver.receiveNext())
        {
            if (lastSeenLappedCount != m_receiver.lappedCount())
            {
                throw util::IllegalArgumentException("Unable to keep up with broadcast buffer", SOURCEINFO);
            }

            const std::int32_t length = m_receiver.length();
            if (length > m_scratchBuffer.capacity())
            {
                throw util::IllegalStateException(
                    util::strPrintf("Buffer required size %d but only has %d", length, m_scratchBuffer.capacity()), SOURCEINFO);
            }

            const std::int32_t msgTypeId = m_receiver.typeId();
            m_scratchBuffer.putBytes(0, m_receiver.buffer(), m_receiver.offset(), length);

            if (!m_receiver.validate())
            {
                throw util::IllegalStateException("Unable to keep up with broadcast buffer", SOURCEINFO);
            }

            handler(msgTypeId, m_scratchBuffer, 0, length);

            messagesReceived = 1;
        }

        return messagesReceived;
    }

private:
    AERON_DECL_ALIGNED(scratch_buffer_t m_scratch, 16);
    BroadcastReceiver& m_receiver;
    AtomicBuffer m_scratchBuffer;
};

}}}

#endif

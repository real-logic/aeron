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

#ifndef INCLUDED_AERON_PUBLICATION__
#define INCLUDED_AERON_PUBLICATION__

#include <iostream>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>

namespace aeron {

using namespace aeron::common;

class ClientConductor;

class Publication
{
friend class ClientConductor;
public:

    struct Identification
    {
        const std::string channel;
        std::int64_t correlationId;
        std::int32_t streamId;
        std::int32_t sessionId;

        Identification(const std::string& channel, std::int64_t correlationId, std::int32_t streamId, std::int32_t sessionId) :
            channel(channel), correlationId(correlationId), streamId(streamId), sessionId(sessionId)
        {
        }
    };

    virtual ~Publication();

    inline const std::string& channel() const
    {
        return m_ident.channel;
    }

    inline std::int32_t streamId() const
    {
        return m_ident.streamId;
    }

    inline std::int32_t sessionId() const
    {
        return m_ident.sessionId;
    }

    inline bool offer(concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        bool succeeded = false;

        return succeeded;
    }

    inline bool offer(concurrent::AtomicBuffer& buffer)
    {
        return offer(buffer, 0, buffer.getCapacity());
    }

    inline bool tryClaim(util::index_t length, concurrent::logbuffer::BufferClaim& bufferClaim)
    {
        bool succeeded = false;

        return succeeded;
    }

private:
    ClientConductor& m_conductor;
    struct Identification m_ident;

    // ClientConductor should only be the one constructing these
    Publication(
        ClientConductor& conductor,
        struct Identification& ident);
};

}

#endif
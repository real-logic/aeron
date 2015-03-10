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

#ifndef INCLUDED_AERON_SUBSCRIPTION__
#define INCLUDED_AERON_SUBSCRIPTION__

#include <cstdint>
#include <iostream>

namespace aeron {

class ClientConductor;

class Subscription
{
friend class ClientConductor;
public:

    virtual ~Subscription();

    inline const std::string& channel() const
    {
        return m_channel;
    }

    inline std::int32_t streamId() const
    {
        return m_streamId;
    }

    inline std::int64_t correlationId() const
    {
        return m_correlationId;
    }

private:
    ClientConductor& m_conductor;
    const std::string& m_channel;
    std::int64_t m_correlationId;
    std::int32_t m_streamId;

    Subscription(ClientConductor& conductor, std::int64_t correlationId, const std::string& channel, std::int32_t streamId);
};

}

#endif
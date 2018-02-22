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

#include "Counter.h"
#include "ClientConductor.h"

namespace aeron
{

Counter::Counter(
    ClientConductor* clientConductor,
    AtomicBuffer& buffer,
    std::int64_t registrationId,
    std::int32_t counterId) :
    AtomicCounter(buffer, counterId),
    m_clientConductor(clientConductor),
    m_registrationId(registrationId)
{
}

Counter::~Counter()
{
    if (nullptr != m_clientConductor)
    {
        m_clientConductor->releaseCounter(m_registrationId);
    }
}

std::int32_t Counter::state() const
{
    return m_clientConductor->countersReader().getCounterState(id());
}

std::string Counter::label() const
{
    return m_clientConductor->countersReader().getCounterLabel(id());
}

}

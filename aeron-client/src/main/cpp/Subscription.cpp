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

#include "Subscription.h"
#include "ClientConductor.h"

namespace aeron {

Subscription::Subscription(
    ClientConductor &conductor, std::int64_t registrationId, const std::string &channel, std::int32_t streamId) :
    m_conductor(conductor),
    m_channel(channel),
    m_registrationId(registrationId),
    m_streamId(streamId),
    m_connections(nullptr),
    m_connectionsLength(0)
{

}

Subscription::~Subscription()
{
    m_conductor.releaseSubscription(m_registrationId);

    // TODO: clean out connections and have client conductor linger them
}

}
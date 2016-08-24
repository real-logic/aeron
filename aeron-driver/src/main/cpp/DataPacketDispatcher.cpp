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

#include <util/Exceptions.h>

#include "DataPacketDispatcher.h"

using namespace aeron::util;


void DataPacketDispatcher::removePendingSetup(int32_t sessionId, int32_t streamId)
{
    const std::pair<int, int> sessionRef{sessionId, streamId};
    auto ignoredSession = m_ignoredSessions.find(sessionRef);

    if (ignoredSession != m_ignoredSessions.end() && ignoredSession->second == PENDING_SETUP_FRAME)
    {
        m_ignoredSessions.erase(sessionRef);
    }
}

void DataPacketDispatcher::removeCoolDown(std::int32_t sessionId, std::int32_t streamId)
{
    auto ignoredItr = m_ignoredSessions.find({sessionId, streamId});
    if (ignoredItr != m_ignoredSessions.end() && ignoredItr->second == ON_COOL_DOWN)
    {
        m_ignoredSessions.erase({sessionId, streamId});
    }
}

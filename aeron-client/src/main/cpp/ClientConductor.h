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

#ifndef INCLUDED_AERON_CLIENT_CONDUCTOR__
#define INCLUDED_AERON_CLIENT_CONDUCTOR__

#include <concurrent/logbuffer/LogReader.h>
#include <vector>
#include "Publication.h"
#include "Subscription.h"

namespace aeron {

using namespace aeron::common::concurrent::logbuffer;

class ClientConductor
{
public:
    ClientConductor()
    {

    }

    int doWork()
    {
        return 0;
    }

    void onClose()
    {
    }

    /*
     * non-blocking API semantics
     * - Publication & Subscription are valid, but no-op offer/poll until successful registration
     * - on error (driver timeout, etc.), deliver notification via errorHandler
     * - next call to method of Publication or Subscription causes exception
     */

    Publication* addPublication(const std::string& channel, std::int32_t streamId, std::int32_t sessionId);
    void releasePublication(Publication* publication);

    Subscription* addSubscription(const std::string& channel, std::int32_t streamId, handler_t& handler);
    void releaseSubscription(Subscription* subscription);

private:
    std::mutex m_publicationsLock;
    std::mutex m_subscriptionsLock;
    std::vector<Publication*> m_publications;
    std::vector<Subscription*> m_subscriptions;
};

}

#endif
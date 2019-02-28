/*
 * Copyright 2014-2019 Real Logic Ltd.
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
#ifndef AERON_ARCHIVE_CONTROLRESPONSEPOLLER_H
#define AERON_ARCHIVE_CONTROLRESPONSEPOLLER_H

#include "Aeron.h"

namespace aeron {
namespace archive {
namespace client {

class ControlResponsePoller
{
public:
    ControlResponsePoller(std::shared_ptr<Subscription> subscription, int fragmentLimit = 10);
    ~ControlResponsePoller();

    inline std::shared_ptr<Subscription> subscription()
    {
        return m_subscription;
    }

    inline void subscription(std::shared_ptr<Subscription> subscription)
    {
        m_subscription = std::move(subscription);
    }
private:
    std::shared_ptr<Subscription> m_subscription;
    const int m_fragmentLimit;

};

}}}
#endif //AERON_ARCHIVE_CONTROLRESPONSEPOLLER_H

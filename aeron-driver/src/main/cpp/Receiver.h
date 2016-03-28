/*
 * Copyright 2015 - 2016 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_DRIVER_RECEIVER_
#define INCLUDED_AERON_DRIVER_RECEIVER_

#include <cstdint>
#include "MediaDriver.h"
#include "media/ReceiveChannelEndpoint.h"

using namespace aeron::driver;
using namespace aeron::driver::media;

namespace aeron { namespace driver {

class Receiver
{
public:
    virtual ~Receiver() = default;

    inline COND_MOCK_VIRTUAL void addPendingSetupMessage(
        std::int32_t sessionId, std::int32_t streamId, ReceiveChannelEndpoint& receiveChannelEndpoint)
    {
    }
};

}};

#endif //AERON_RECEIVER_H

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

#ifndef AERON_PUBLICATIONIMAGE_H
#define AERON_PUBLICATIONIMAGE_H

#include <cstdint>
#include <concurrent/AtomicBuffer.h>
#include <util/MacroUtil.h>

namespace aeron { namespace driver
{

using namespace aeron::concurrent;

class PublicationImage
{
public:

    typedef std::shared_ptr<PublicationImage> ptr_t;

    PublicationImage(){}
    virtual ~PublicationImage(){}

    std::int32_t sessionId();
    std::int32_t streamId();

    virtual std::int32_t insertPacket(
        std::int32_t termId, std::int32_t termOffset, AtomicBuffer& buffer, std::int32_t length);

};

}};

#endif //AERON_PUBLICATIONIMAGE_H

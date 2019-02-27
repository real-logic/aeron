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
#ifndef AERON_ARCHIVE_AERONARCHIVE_H
#define AERON_ARCHIVE_AERONARCHIVE_H

#include "Aeron.h"
#include "ArchiveConfiguration.h"
#include "concurrent/BackOffIdleStrategy.h"

namespace aeron {
namespace archive {
namespace client {

template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
class AeronArchive
{
public:
    using Context_t = aeron::archive::client::Context;

    AeronArchive(Context_t& context);
    ~AeronArchive();

    inline static std::shared_ptr<AeronArchive> connect(Context_t& context)
    {
        return std::make_shared<AeronArchive>(context);
    }

    inline static std::shared_ptr<AeronArchive> connect()
    {
        Context_t ctx;
        return connect(ctx);
    }

private:
    std::shared_ptr<Aeron> m_aeron;
    Context_t m_context;
    IdleStrategy m_idle;
};

}}}
#endif //AERON_ARCHIVE_AERONARCHIVE_H

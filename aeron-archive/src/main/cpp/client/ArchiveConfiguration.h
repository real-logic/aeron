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
#ifndef AERON_ARCHIVE_CONFIGURATION_H
#define AERON_ARCHIVE_CONFIGURATION_H

#include <cstdint>

#include "Aeron.h"
#include "util/MacroUtil.h"

namespace aeron {
namespace archive {
namespace client {

constexpr const std::int64_t NULL_TIMESTAMP = aeron::NULL_VALUE;
constexpr const std::int64_t NULL_POSITION = aeron::NULL_VALUE;
constexpr const std::int64_t NULL_LENGTH = aeron::NULL_VALUE;

namespace Configuration
{
constexpr const std::uint8_t ARCHIVE_MAJOR_VERSION = 0;
constexpr const std::uint8_t ARCHIVE_MINOR_VERSION = 1;
constexpr const std::uint8_t ARCHIVE_PATCH_VERSION = 1;
constexpr const std::int32_t ARCHIVE_SEMANTIC_VERSION = aeron::util::semanticVersionCompose(
    ARCHIVE_MAJOR_VERSION, ARCHIVE_MINOR_VERSION, ARCHIVE_PATCH_VERSION);

constexpr const long long MESSAGE_TIMEOUT_NS_DEFAULT = 5 * 1000 * 1000 * 1000L;

}

class Context
{
public:
    using this_t = Context;

    Context() : m_aeronDirectoryName(aeron::Context::defaultAeronPath())
    {
    }

    void conclude()
    {
        if (!m_aeron)
        {
            aeron::Context ctx;

            ctx.aeronDir(m_aeronDirectoryName);
            m_aeron = Aeron::connect(ctx);
            m_ownsAeronClient = true;
        }
    }

    inline std::shared_ptr<Aeron> aeron()
    {
        return m_aeron;
    }

    inline this_t& aeron(std::shared_ptr<Aeron> aeron)
    {
        m_aeron = std::move(aeron);
        return *this;
    }

    inline long long messageTimeoutNs()
    {
        return m_messageTimeoutNs;
    }

    inline this_t& messageTimeoutNs(long long timeoutNs)
    {
        m_messageTimeoutNs = timeoutNs;
        return *this;
    }

private:
    std::shared_ptr<Aeron> m_aeron;
    std::string m_aeronDirectoryName;
    long long m_messageTimeoutNs = Configuration::MESSAGE_TIMEOUT_NS_DEFAULT;
    bool m_ownsAeronClient = false;
};

}}}
#endif //AERON_ARCHIVE_CONFIGURATION_H

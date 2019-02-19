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

#ifndef AERON_ARCHIVE_ARCHIVEPROXY_H
#define AERON_ARCHIVE_ARCHIVEPROXY_H

#include "Aeron.h"

namespace aeron {
namespace archive {
namespace client {

class ArchiveProxy
{
public:
    ArchiveProxy(
        std::shared_ptr<Publication> publication,
        nano_clock_t nanoClock,
        long messageTimeoutNs)
        :
        m_publication(std::move(publication)),
        m_nanoClock(std::move(nanoClock)),
        m_messageTimeoutNs(messageTimeoutNs)
    {
    }

    bool connect(
        const std::string& responseChannel,
        std::int32_t responseStreamId,
        std::int64_t correlationId,
        std::shared_ptr<Aeron> aeron);

private:
    std::shared_ptr<Publication> m_publication;
    nano_clock_t m_nanoClock;
    const long m_messageTimeoutNs;

    bool tryClaimWithTimeout(
        std::int32_t length, BufferClaim& bufferClaim, std::shared_ptr<Aeron> aeron);
};

}}};

#endif //AERON_ARCHIVEPROXY_H

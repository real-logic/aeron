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
#include "concurrent/BackOffIdleStrategy.h"

namespace aeron {
namespace archive {
namespace client {

class ArchiveProxy
{
public:
    ArchiveProxy(
        std::shared_ptr<ExclusivePublication> publication,
        nano_clock_t nanoClock,
        long long messageTimeoutNs,
        int retryAttempts = 3) :
        m_publication(std::move(publication)),
        m_nanoClock(std::move(nanoClock)),
        m_messageTimeoutNs(messageTimeoutNs),
        m_retryAttempts(retryAttempts)
    {
    }

    inline std::shared_ptr<ExclusivePublication> publication()
    {
        return m_publication;
    }

    bool tryConnect(const std::string& responseChannel, std::int32_t responseStreamId, std::int64_t correlationId);

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool replay(
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        const std::string& replayChannel,
        std::int32_t replayStreamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

private:
    std::shared_ptr<ExclusivePublication> m_publication;
    nano_clock_t m_nanoClock;
    const long long m_messageTimeoutNs;
    const int m_retryAttempts;

    template<typename IdleStrategy>
    bool tryClaim(std::int32_t length, BufferClaim& bufferClaim);
};

}}};

#endif //AERON_ARCHIVEPROXY_H

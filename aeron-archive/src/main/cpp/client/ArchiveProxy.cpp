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

#include "ArchiveProxy.h"
#include "concurrent/YieldingIdleStrategy.h"
#include "aeron_archive_client/ConnectRequest.h"

using namespace aeron::concurrent;
using namespace aeron::archive::client;

bool ArchiveProxy::connect(
    const std::string& responseChannel,
    std::int32_t responseStreamId,
    std::int64_t correlationId,
    std::shared_ptr<Aeron> aeron)
{
    const std::size_t length = ConnectRequest::sbeBlockLength()
        + ConnectRequest::responseChannelHeaderLength()
        + responseChannel.size();

    BufferClaim bufferClaim;

    if (tryClaimWithTimeout(static_cast<std::int32_t>(length), bufferClaim, std::move(aeron)))
    {
        ConnectRequest connectRequest;

        connectRequest
            .wrapAndApplyHeader(reinterpret_cast<char *>(bufferClaim.buffer().buffer()), 0, length)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .version(0x0)
            .putResponseChannel(responseChannel);

        bufferClaim.commit();
        return true;
    }

    return false;
}

bool ArchiveProxy::tryClaimWithTimeout(
    std::int32_t length, BufferClaim& bufferClaim, std::shared_ptr<Aeron> aeron)
{
    YieldingIdleStrategy idleStrategy;

    const std::int64_t deadlineNs = m_nanoClock() + m_messageTimeoutNs;
    while (true)
    {
        const std::int64_t result = m_publication->tryClaim(length, bufferClaim);
        if (result > 0)
        {
            return true;
        }

        if (result == PUBLICATION_CLOSED)
        {
            throw util::IllegalArgumentException("connection to the archive has been closed", SOURCEINFO);
        }

        if (result == MAX_POSITION_EXCEEDED)
        {
            throw util::IllegalArgumentException("offer failed due to max position being reached", SOURCEINFO);
        }

        if (deadlineNs - m_nanoClock() < 0)
        {
            return false;
        }

        if (aeron->usesAgentInvoker())
        {
            aeron->conductorAgentInvoker().invoke();
        }

        idleStrategy.idle();
    }
}
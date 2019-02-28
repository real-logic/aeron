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
#include "ControlledFragmentAssembler.h"

namespace aeron {
namespace archive {
namespace client {

class ControlResponsePoller
{
public:
    explicit ControlResponsePoller(std::shared_ptr<Subscription> subscription, int fragmentLimit = 10);

    inline std::shared_ptr<Subscription> subscription()
    {
        return m_subscription;
    }

    inline int poll()
    {
        m_controlSessionId = -1;
        m_correlationId = -1;
        m_relevantId = -1;
        m_templateId = -1;
        m_errorMessage = "";
        m_pollComplete = false;
        m_isCodeOk = false;
        m_isCodeError = false;
        m_isControlResponse = false;

        return m_subscription->controlledPoll(m_fragmentHandler, m_fragmentLimit);
    }

    inline std::int64_t controlSessionId()
    {
        return m_controlSessionId;
    }

    inline std::int64_t correlationId()
    {
        return m_correlationId;
    }

    inline std::int64_t relevantId()
    {
        return m_relevantId;
    }

    inline std::int64_t templateId()
    {
        return m_templateId;
    }

    inline bool isControlResponse()
    {
        return m_isControlResponse;
    }

    inline bool isPollComplete()
    {
        return m_pollComplete;
    }

    inline std::string errorMessage()
    {
        return m_errorMessage;
    }

    inline bool isCodeOk()
    {
        return m_isCodeOk;
    }

    inline bool isCodeError()
    {
        return m_isCodeError;
    }

    inline int codeValue()
    {
        return m_codeValue;
    }

    ControlledPollAction onFragment(AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header);

private:
    ControlledFragmentAssembler m_fragmentAssembler;
    controlled_poll_fragment_handler_t m_fragmentHandler;
    std::shared_ptr<Subscription> m_subscription;
    const int m_fragmentLimit;

    std::int64_t m_controlSessionId = -1;
    std::int64_t m_correlationId = -1;
    std::int64_t m_relevantId = -1;
    std::int16_t m_templateId = -1;
    std::string m_errorMessage = "";
    int m_codeValue = -1;
    bool m_pollComplete = false;
    bool m_isCodeOk = false;
    bool m_isCodeError = false;
    bool m_isControlResponse = false;
};

}}}
#endif //AERON_ARCHIVE_CONTROLRESPONSEPOLLER_H

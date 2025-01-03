/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef AERON_ARCHIVE_RECORDING_SIGNAL_ADAPTER_H
#define AERON_ARCHIVE_RECORDING_SIGNAL_ADAPTER_H

#include "ArchiveConfiguration.h"
#include "ControlledFragmentAssembler.h"
#include "ControlResponseAdapter.h"

namespace aeron { namespace archive { namespace client
{

class RecordingSignalAdapter
{
public:
    /**
     * Create an adapter for a given subscription to an archive for control response messages and
     * recording operation signals for a given archive session.
     *
     * @param onResponse        to which control responses are dispatched.
     * @param onRecordingSignal to which recording signals are dispatched.
     * @param subscription      to poll for new events.
     * @param controlSessionId  to filter on.
     * @param fragmentLimit     to apply for each polling operation.
     */
    RecordingSignalAdapter(
        const on_control_response_t &onResponse,
        const on_recording_signal_t &onRecordingSignal,
        std::shared_ptr<Subscription> subscription,
        std::int64_t controlSessionId,
        int fragmentLimit = 10);

    /**
     * Get the Subscription used for polling messages.
     *
     * @return the Subscription used for polling messages.
     */
    inline std::shared_ptr<Subscription> subscription()
    {
        return m_subscription;
    }

    /**
     * Get the session id associated with the archive control session to filter on.
     *
     * @return the session id associated with the archive control session to filter on.
     */
    inline std::int64_t controlSessionId() const
    {
        return m_controlSessionId;
    }

    /**
     * Poll for recording events and dispatch them to the callbacks for this instance.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    inline int poll()
    {
        if (m_isAbort)
        {
            m_isAbort = false;
        }

        return m_subscription->controlledPoll(m_fragmentHandler, m_fragmentLimit);
    }

    ControlledPollAction onFragment(AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header);

private:
    ControlledFragmentAssembler m_fragmentAssembler;
    controlled_poll_fragment_handler_t m_fragmentHandler;
    std::shared_ptr<Subscription> m_subscription;
    on_control_response_t m_onResponse;
    on_recording_signal_t m_onRecordingSignal;
    const std::int64_t m_controlSessionId;
    const int m_fragmentLimit;
    bool m_isAbort = false;
};

}}}

#endif //AERON_ARCHIVE_RECORDING_SIGNAL_ADAPTER_H

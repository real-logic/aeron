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
#ifndef AERON_RECORDING_EVENTS_ADAPTER_H
#define AERON_RECORDING_EVENTS_ADAPTER_H

#include "Aeron.h"

namespace aeron {
namespace archive {
namespace client {

typedef std::function<void(
    std::int64_t recordingId,
    std::int64_t startPosition,
    std::int32_t sessionId,
    std::int32_t streamId,
    const std::string& channel,
    const std::string& sourceIdentity)> on_recording_start_t;

typedef std::function<void(
    std::int64_t recordingId,
    std::int64_t startPosition,
    std::int64_t position)> on_recording_event_t;

class RecordingEventsAdapter
{
public:
    RecordingEventsAdapter(
        const on_recording_start_t& onStart,
        const on_recording_event_t& onProgress,
        const on_recording_event_t& onStop,
        std::shared_ptr<Subscription> subscription,
        int fragmentLimit = 10);

    inline std::shared_ptr<Subscription> subscription()
    {
        return m_subscription;
    }

    inline int poll()
    {
        return m_subscription->poll(m_fragmentHandler, m_fragmentLimit);
    }

    void onFragment(AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header);

private:
    fragment_handler_t m_fragmentHandler;
    std::shared_ptr<Subscription> m_subscription;
    on_recording_start_t m_onStart;
    on_recording_event_t m_onProgress;
    on_recording_event_t m_onStop;
    const int m_fragmentLimit;
};

}}}
#endif //AERON_RECORDING_EVENTS_ADAPTER_H

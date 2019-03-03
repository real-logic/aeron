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
#ifndef AERON_RECORDING_EVENTS_POLLER_H
#define AERON_RECORDING_EVENTS_POLLER_H

#include "Aeron.h"

namespace aeron {
namespace archive {
namespace client {

class RecordingEventsPoller
{
public:
    enum EventType: std::uint8_t
    {
        RECORDING_STARTTED = 1,
        RECORDING_PROGRESS = 2,
        RECORDING_STOPPED = 3,
        UNKNOWN_EVENT = 255
    };

    explicit RecordingEventsPoller(std::shared_ptr<Subscription> subscription);

    inline std::shared_ptr<Subscription> subscription()
    {
        return m_subscription;
    }

    inline int poll()
    {
        m_eventType = EventType::UNKNOWN_EVENT;
        m_pollComplete = false;

        return m_subscription->poll(m_fragmentHandler, 1);
    }

    inline bool isPollComplete()
    {
        return m_pollComplete;
    }

    inline EventType eventType()
    {
        return m_eventType;
    }

    inline std::int64_t recordingId()
    {
        return m_recordingId;
    }

    inline std::int64_t recordingStartPosition()
    {
        return m_recordingStartPosition;
    }

    inline std::int64_t recordingPosition()
    {
        return m_recordingPosition;
    }

    inline std::int64_t recordingStopPosition()
    {
        return m_recordingStopPosition;
    }

    void onFragment(AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header);

private:
    fragment_handler_t m_fragmentHandler;
    std::shared_ptr<Subscription> m_subscription;

    std::int64_t m_recordingId = -1;
    std::int64_t m_recordingStartPosition = -1;
    std::int64_t m_recordingPosition = -1;
    std::int64_t m_recordingStopPosition = -1;
    EventType m_eventType = EventType::UNKNOWN_EVENT;
    bool m_pollComplete = false;
};

}}}
#endif //AERON_RECORDING_EVENTS_POLLER_H

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

namespace aeron { namespace archive { namespace client {

/**
 * Encapsulate the polling and decoding of recording events.
 */
class RecordingEventsPoller
{
public:
    /// Type of recording event.
    enum EventType: std::uint8_t
    {
        RECORDING_STARTED = 1,
        RECORDING_PROGRESS = 2,
        RECORDING_STOPPED = 3,
        UNKNOWN_EVENT = 255
    };

    explicit RecordingEventsPoller(std::shared_ptr<Subscription> subscription);

    /**
     * Get the Subscription used for polling recording events.
     *
     * @return the Subscription used for polling recording events.
     */
    inline std::shared_ptr<Subscription> subscription()
    {
        return m_subscription;
    }

    /**
     * Poll for recording events.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    inline int poll()
    {
        m_eventType = EventType::UNKNOWN_EVENT;
        m_pollComplete = false;

        return m_subscription->poll(m_fragmentHandler, 1);
    }

    /**
     * Has the last polling action received a complete message?
     *
     * @return true of the last polling action received a complete message?
     */
    inline bool isPollComplete()
    {
        return m_pollComplete;
    }

    /**
     * Get the EventType of the last recording event.
     *
     * @return the EventType of the last recording event.
     */
    inline EventType eventType()
    {
        return m_eventType;
    }

    /**
     * Get the recording id of the last received event.
     *
     * @return the recording id of the last received event.
     */
    inline std::int64_t recordingId()
    {
        return m_recordingId;
    }

    /**
     * Get the position the recording started at.
     *
     * @return the position the recording started at.
     */
    inline std::int64_t recordingStartPosition()
    {
        return m_recordingStartPosition;
    }

    /**
     * Get the current recording position.
     *
     * @return the current recording position.
     */
    inline std::int64_t recordingPosition()
    {
        return m_recordingPosition;
    }

    /**
     * Get the position the recording stopped at.
     *
     * @return the position the recording stopped at.
     */
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

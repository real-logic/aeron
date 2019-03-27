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

namespace aeron { namespace archive { namespace client {

/**
 * Fired when a recording is started.
 *
 * @param recordingId    assigned to the new recording.
 * @param startPosition  in the stream at which the recording started.
 * @param sessionId      of the publication being recorded.
 * @param streamId       of the publication being recorded.
 * @param channel        of the publication being recorded.
 * @param sourceIdentity of the publication being recorded.
 */
typedef std::function<void(
    std::int64_t recordingId,
    std::int64_t startPosition,
    std::int32_t sessionId,
    std::int32_t streamId,
    const std::string& channel,
    const std::string& sourceIdentity)> on_recording_start_t;

/**
 * Progress indication of an active recording or Indication of a stopped recording.
 *
 * @param recordingId   for which progress or stop is being reported.
 * @param startPosition in the stream at which the recording started.
 * @param position      reached in recording the publication.
 */
typedef std::function<void(
    std::int64_t recordingId,
    std::int64_t startPosition,
    std::int64_t position)> on_recording_event_t;

/**
 * Encapsulate the polling, decoding, and dispatching of recording events.
 */
class RecordingEventsAdapter
{
public:
    /**
     * Create an adapter for a given subscription to an archive for recording events.
     *
     * @param onStart to call when a recording started event is received.
     * @param onProgress to call when a recording progress event is received.
     * @param onStop to call when a recording stopped event is received.
     * @param subscription  to poll for new events.
     * @param fragmentLimit to apply for each polling operation.
     */
    RecordingEventsAdapter(
        const on_recording_start_t& onStart,
        const on_recording_event_t& onProgress,
        const on_recording_event_t& onStop,
        std::shared_ptr<Subscription> subscription,
        int fragmentLimit = 10);

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
     * Poll for recording events and dispatch them to the callbacks for this instance.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
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

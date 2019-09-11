/*
 * Copyright 2014-2019 Real Logic Ltd.
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
#ifndef AERON_ARCHIVE_RECORDING_TRANSITION_ADAPTER_H
#define AERON_ARCHIVE_RECORDING_TRANSITION_ADAPTER_H

#include "Aeron.h"
#include "ControlResponseAdapter.h"
#include "aeron_archive_client/RecordingTransitionType.h"

namespace aeron { namespace archive { namespace client {

/**
 * An event has been received from the Archive indicating a lifecycle transition for a recording.
 *
 * @param controlSessionId of the originating session.
 * @param recordingId      of the recording which transitioned.
 * @param subscriptionId   of the subscription which captured the recording.
 * @param position         of the recording at the time of transition.
 * @param transitionType   type of transition the recording has undertaken.
 */
typedef std::function<void(
    std::int64_t controlSessionId,
    std::int64_t recordingId,
    std::int64_t subscriptionId,
    std::int64_t position,
    RecordingTransitionType::Value transitionType)> on_recording_transition_t;

class RecordingTransitionAdapter
{
public:
    /**
     * Create an adapter for a given subscription to an archive for control response messages and
     * recording lifecycle transition for a given archive session.
     *
     * @param onResponse            to which control responses are dispatched.
     * @param onRecordingTransition to which recording descriptors are dispatched.
     * @param subscription          to poll for new events.
     * @param controlSessionId      to filter on.
     * @param fragmentLimit to apply for each polling operation.
     */
    RecordingTransitionAdapter(
        const on_control_response_t &onResponse,
        const on_recording_transition_t &onRecordingTransition,
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
        return m_subscription->poll(m_fragmentHandler, m_fragmentLimit);
    }

    void onFragment(AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header);

private:
    fragment_handler_t m_fragmentHandler;
    std::shared_ptr<Subscription> m_subscription;
    on_control_response_t m_onResponse;
    on_recording_transition_t m_onRecordingTransition;
    const std::int64_t m_controlSessionId;
    const int m_fragmentLimit;
};

}}}
#endif //AERON_ARCHIVE_RECORDING_TRANSITION_ADAPTER_H

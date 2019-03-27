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
#ifndef AERON_ARCHIVE_CONTROL_RESPONSE_ADAPTER_H
#define AERON_ARCHIVE_CONTROL_RESPONSE_ADAPTER_H

#include "Aeron.h"
#include "RecordingDescriptorPoller.h"
#include "RecordingEventsAdapter.h"

namespace aeron { namespace archive { namespace client {

/// Control Response Code of OK
constexpr const std::int32_t CONTROL_RESPONSE_CODE_OK = 0;
/// Control Response Code of ERROR
constexpr const std::int32_t CONTROL_RESPONSE_CODE_ERROR = 1;
/// Control Response Code of RECORDING_UNKNOWN
constexpr const std::int32_t CONTROL_RESPONSE_CODE_RECORDING_UNKNOWN = 2;
/// Control Response Code of SUBSCRIPTION_UNKNOWN
constexpr const std::int32_t CONTROL_RESPONSE_CODE_SUBSCRIPTION_UNKNOWN = 3;

/**
 * An event has been received from the Archive in response to a request with a given correlation id.
 *
 * @param controlSessionId of the originating session.
 * @param correlationId    of the associated request.
 * @param relevantId       of the object to which the response applies.
 * @param code             for the response status.
 * @param errorMessage     when is set if the response code is not OK.
 */
typedef std::function<void(
    std::int64_t controlSessionId,
    std::int64_t correlationId,
    std::int64_t relevantId,
    std::int32_t code,
    const std::string& errorMessage)> on_control_response_t;

class ControlResponseAdapter
{
public:
    /**
     * Create an adapter for a given subscription to an archive for control response messages.
     *
     * @param onResponse    to which control responses are dispatched.
     * @param onRecordingDescriptor to which recording descriptors are dispatched.
     * @param subscription  to poll for new events.
     * @param fragmentLimit to apply for each polling operation.
     */
    ControlResponseAdapter(
        const on_control_response_t& onResponse,
        const recording_descriptor_consumer_t& onRecordingDescriptor,
        std::shared_ptr<Subscription> subscription,
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
    recording_descriptor_consumer_t m_onRecordingDescriptor;
    const int m_fragmentLimit;
};

}}}
#endif //AERON_ARCHIVE_CONTROL_RESPONSE_ADAPTER_H

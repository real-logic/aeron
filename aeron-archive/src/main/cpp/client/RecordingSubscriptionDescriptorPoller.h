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
#ifndef AERON_RECORDING_SUBSCRIPTION_DESCRIPTOR_POLLER_H
#define AERON_RECORDING_SUBSCRIPTION_DESCRIPTOR_POLLER_H

#include "Aeron.h"
#include "ControlledFragmentAssembler.h"

namespace aeron { namespace archive { namespace client
{

/**
 * Descriptor for an active recording subscription on the archive.
 *
 * @param controlSessionId for the request.
 * @param correlationId    for the request.
 * @param subscriptionId   that can be used to stop the recording subscription.
 * @param streamId         the subscription was registered with.
 * @param strippedChannel  the subscription was registered with.
 */
typedef std::function<void(
    std::int64_t controlSessionId,
    std::int64_t correlationId,
    std::int64_t subscriptionId,
    std::int32_t streamId,
    const std::string& strippedChannel)> recording_subscription_descriptor_consumer_t;

/**
 * Encapsulate the polling, decoding, dispatching of recording descriptors from an archive.
 */
class RecordingSubscriptionDescriptorPoller
{
public:
    RecordingSubscriptionDescriptorPoller(
        std::shared_ptr<Subscription> subscription,
        const exception_handler_t& errorHandler,
        std::int64_t controlSessionId,
        int fragmentLimit = 10);

    /**
     * Poll for recording subscriptions.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    inline int poll()
    {
        m_isDispatchComplete = false;

        return m_subscription->controlledPoll(m_fragmentHandler, m_fragmentLimit);
    }

    /**
     * Get the Subscription used for polling responses.
     *
     * @return the Subscription used for polling responses.
     */
    inline std::shared_ptr<Subscription> subscription()
    {
        return m_subscription;
    }

    /**
     * Control session id for filtering responses.
     *
     * @return control session id for filtering responses.
     */
    inline std::int64_t controlSessionId()
    {
        return m_controlSessionId;
    }

    /**
     * Is the dispatch of descriptors complete?
     *
     * @return true if the dispatch of descriptors complete?
     */
    inline bool isDispatchComplete()
    {
        return m_isDispatchComplete;
    }

    /**
     * Get the number of remaining subscriptions expected.
     *
     * @return the number of remaining subscriptions expected.
     */
    inline std::int32_t remainingSubscriptionCount()
    {
        return m_remainingSubscriptionCount;
    }

    /**
     * Reset the poller to dispatch the descriptors returned from a query.
     *
     * @param correlationId     for the response.
     * @param subscriptionCount of descriptors to expect.
     * @param consumer          to which the recording subscription descriptors are to be dispatched.
     */
    inline void reset(
        std::int64_t correlationId,
        std::int32_t subscriptionCount,
        const recording_subscription_descriptor_consumer_t& consumer)
    {
        m_correlationId = correlationId;
        m_remainingSubscriptionCount = subscriptionCount;
        m_consumer = consumer;
        m_isDispatchComplete = false;
    }

    ControlledPollAction onFragment(AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header);

private:
    ControlledFragmentAssembler m_fragmentAssembler;
    controlled_poll_fragment_handler_t m_fragmentHandler;
    exception_handler_t m_errorHandler;
    recording_subscription_descriptor_consumer_t m_consumer = nullptr;
    std::shared_ptr<Subscription> m_subscription;

    const std::int64_t m_controlSessionId;
    const int m_fragmentLimit;

    std::int64_t m_correlationId = -1;
    std::int32_t m_remainingSubscriptionCount = 0;
    bool m_isDispatchComplete = false;
};

}}}
#endif //AERON_RECORDING_SUBSCRIPTION_DESCRIPTOR_POLLER_H

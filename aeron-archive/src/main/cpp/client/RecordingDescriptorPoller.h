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
#ifndef AERON_RECORDING_DESCRIPTOR_POLLER_H
#define AERON_RECORDING_DESCRIPTOR_POLLER_H

#include "Aeron.h"
#include "ControlledFragmentAssembler.h"

namespace aeron { namespace archive { namespace client
{

/**
 * A recording descriptor returned as a result of requesting a listing of recordings.
 *
 * @param controlSessionId  of the originating session requesting to list recordings.
 * @param correlationId     of the associated request to list recordings.
 * @param recordingId       of this recording descriptor.
 * @param startTimestamp    for the recording.
 * @param stopTimestamp     for the recording.
 * @param startPosition     for the recording against the recorded publication.
 * @param stopPosition      reached for the recording.
 * @param initialTermId     for the recorded publication.
 * @param segmentFileLength for the recording which is a multiple of termBufferLength.
 * @param termBufferLength  for the recorded publication.
 * @param mtuLength         for the recorded publication.
 * @param sessionId         for the recorded publication.
 * @param streamId          for the recorded publication.
 * @param strippedChannel   for the recorded publication.
 * @param originalChannel   for the recorded publication.
 * @param sourceIdentity    for the recorded publication.
 */
typedef std::function<void(
    std::int64_t controlSessionId,
    std::int64_t correlationId,
    std::int64_t recordingId,
    std::int64_t startTimestamp,
    std::int64_t stopTimestamp,
    std::int64_t startPosition,
    std::int64_t stopPosition,
    std::int32_t initialTermId,
    std::int32_t segmentFileLength,
    std::int32_t termBufferLength,
    std::int32_t mtuLength,
    std::int32_t sessionId,
    std::int32_t streamId,
    const std::string& strippedChannel,
    const std::string& originalChannel,
    const std::string& sourceIdentity)> recording_descriptor_consumer_t;

class RecordingDescriptorPoller
{
public:
    RecordingDescriptorPoller(
        std::shared_ptr<Subscription> subscription,
        const exception_handler_t& errorHandler,
        std::int64_t controlSessionId,
        int fragmentLimit = 10);

    /**
     * Poll for recording events.
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
     * Get the number of remaining records are expected.
     *
     * @return the number of remaining records are expected.
     */
    inline std::int32_t remainingRecordCount()
    {
        return m_remainingRecordCount;
    }

    /**
     * Reset the poller to dispatch the descriptors returned from a query.
     *
     * @param correlationId for the response.
     * @param recordCount   of descriptors to expect.
     * @param consumer      to which the recording descriptors are to be dispatched.
     */
    inline void reset(
        std::int64_t correlationId,
        std::int32_t recordCount,
        const recording_descriptor_consumer_t& consumer)
    {
        m_correlationId = correlationId;
        m_remainingRecordCount = recordCount;
        m_consumer = consumer;
        m_isDispatchComplete = false;
    }

    ControlledPollAction onFragment(AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header);

private:
    ControlledFragmentAssembler m_fragmentAssembler;
    controlled_poll_fragment_handler_t m_fragmentHandler;
    exception_handler_t m_errorHandler;
    recording_descriptor_consumer_t m_consumer = nullptr;
    std::shared_ptr<Subscription> m_subscription;

    const std::int64_t m_controlSessionId;
    const int m_fragmentLimit;

    std::int64_t m_correlationId = -1;
    std::int32_t m_remainingRecordCount = 0;
    bool m_isDispatchComplete = false;
};

}}}
#endif //AERON_RECORDING_DESCRIPTOR_POLLER_H

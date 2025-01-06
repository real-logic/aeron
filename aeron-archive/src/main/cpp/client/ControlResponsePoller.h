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
#ifndef AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_H
#define AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_H

#include <utility>

#include "Aeron.h"
#include "ControlledFragmentAssembler.h"

namespace aeron { namespace archive { namespace client
{

/**
 * Encapsulate the polling and decoding of archive control protocol response messages.
 */
class ControlResponsePoller
{
public:
    explicit ControlResponsePoller(std::shared_ptr<Subscription> subscription, int fragmentLimit = 10);

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
     * Poll for control response events.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    inline int poll()
    {
        if (m_isPollComplete)
        {
            m_controlSessionId = aeron::NULL_VALUE;
            m_correlationId = aeron::NULL_VALUE;
            m_relevantId = aeron::NULL_VALUE;
            m_recordingId = aeron::NULL_VALUE;
            m_subscriptionId = aeron::NULL_VALUE;
            m_position = aeron::NULL_VALUE;
            m_recordingSignalCode = INT32_MIN;
            m_version = 0;
            m_codeValue = aeron::NULL_VALUE;
            m_errorMessage = "";
            m_isCodeOk = false;
            m_isCodeError = false;
            m_isControlResponse = false;
            m_wasChallenged = false;
            m_isRecordingSignal = false;
            delete[] m_encodedChallenge.first;
            m_encodedChallenge.first = nullptr;
            m_encodedChallenge.second = 0;
            m_isPollComplete = false;
        }

        return m_subscription->controlledPoll(m_fragmentHandler, m_fragmentLimit);
    }

    /**
     * Control session id of the last polled message or Aeron#NULL_VALUE if poll returned nothing.
     *
     * @return control session id of the last polled message or Aeron#NULL_VALUE if poll returned nothing.
     */
    inline std::int64_t controlSessionId() const
    {
        return m_controlSessionId;
    }

    /**
     * Correlation id of the last polled message or Aeron#NULL_VALUE if poll returned nothing.
     *
     * @return correlation id of the last polled message or Aeron#NULL_VALUE if poll returned nothing.
     */
    inline std::int64_t correlationId() const
    {
        return m_correlationId;
    }

    /**
     * Get the relevant id returned with the response, e.g. replay session id.
     *
     * @return the relevant id returned with the response.
     */
    inline std::int64_t relevantId() const
    {
        return m_relevantId;
    }

    /**
     * Recording id of polled RecordingSignal or Aeron#NULL_VALUE if poll returned nothing.
     *
     * @return recording id of polled RecordingSignal or Aeron#NULL_VALUE if poll returned nothing.
     */
    inline std::int64_t recordingId() const
    {
        return m_recordingId;
    }

    /**
     * Subscription id of polled RecordingSignal or Aeron#NULL_VALUE if poll returned nothing.
     *
     * @return subscription id of polled RecordingSignal or Aeron#NULL_VALUE if poll returned nothing.
     */
    inline std::int64_t subscriptionId() const
    {
        return m_subscriptionId;
    }

    /**
     * Position of polled RecordingSignal or Aeron#NULL_VALUE if poll returned nothing.
     *
     * @return position id of polled RecordingSignal or Aeron#NULL_VALUE if poll returned nothing.
     */
    inline std::int64_t position() const
    {
        return m_position;
    }

    /**
     * Code of polled RecordingSignal or INT32_MIN if poll returned nothing.
     *
     * @return code of polled RecordingSignal or INT32_MIN if poll returned nothing.
     */
    inline std::int32_t recordingSignalCode() const
    {
        return m_recordingSignalCode;
    }

    /**
     * Version response from the server in semantic version form.
     *
     * @return response from the server in semantic version form.
     */
    inline std::int32_t version() const
    {
        return m_version;
    }

    /**
     * Was last received message a Control Response?
     *
     * @return whether the last received message was a Control Response.
     */
    inline bool isControlResponse() const
    {
        return m_isControlResponse;
    }

    /**
     * Was last received message a RecordingSignal?
     *
     * @return whether the last received message was a RecordingSignal.
     */
    inline bool isRecordingSignal() const
    {
        return m_isRecordingSignal;
    }

    /**
     * Was the last polling action received a complete message?
     *
     * @return true if the last polling action received a complete message?
     */
    inline bool isPollComplete() const
    {
        return m_isPollComplete;
    }

    /**
     * Get the error message of the last response.
     *
     * @return the error message of the last response.
     */
    inline std::string errorMessage()
    {
        return m_errorMessage;
    }

    /**
     * Did the last received control response have a response code of OK?
     *
     * @return whether the last received control response had a response code of OK?
     */
    inline bool isCodeOk() const
    {
        return m_isCodeOk;
    }

    /**
     * Did the last received control response have a response code of ERROR?
     *
     * @return whether the last received control response had a response code of ERROR?
     */
    inline bool isCodeError() const
    {
        return m_isCodeError;
    }

    /**
     * Get the response code value of the last response.
     *
     * @return the response code value of the last response.
     */
    inline int codeValue() const
    {
        return m_codeValue;
    }

    /**
     * Was the last polling action received a challenge message?
     *
     * @return true if the last polling action received was a challenge message, false if not.
     */
    inline bool wasChallenged() const
    {
        return m_wasChallenged;
    }

    /**
     * Get the encoded challenge of the last challenge.
     *
     * @return the encoded challenge of the last challenge.
     */
    inline std::pair<const char *, std::uint32_t> encodedChallenge()
    {
        return m_encodedChallenge;
    }

    ControlledPollAction onFragment(AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header);

private:
    ControlledFragmentAssembler m_fragmentAssembler;
    controlled_poll_fragment_handler_t m_fragmentHandler;
    std::shared_ptr<Subscription> m_subscription;
    const int m_fragmentLimit;

    std::int64_t m_controlSessionId = aeron::NULL_VALUE;
    std::int64_t m_correlationId = aeron::NULL_VALUE;
    std::int64_t m_relevantId = aeron::NULL_VALUE;
    std::int64_t m_recordingId = aeron::NULL_VALUE;
    std::int64_t m_subscriptionId = aeron::NULL_VALUE;
    std::int64_t m_position = aeron::NULL_VALUE;
    std::int32_t m_recordingSignalCode = INT32_MIN;
    std::int32_t m_version = 0;
    int m_codeValue = -1;
    std::string m_errorMessage;

    bool m_isPollComplete = false;
    bool m_isCodeOk = false;
    bool m_isCodeError = false;
    bool m_isControlResponse = false;
    bool m_wasChallenged = false;
    bool m_isRecordingSignal = false;
    std::pair<const char *, std::uint32_t> m_encodedChallenge = { nullptr, 0 };
};

}}}

#endif //AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_H

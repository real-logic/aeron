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

#include "ControlResponsePoller.h"
#include "ArchiveException.h"
#include "aeron_archive_client/MessageHeader.h"
#include "aeron_archive_client/ControlResponse.h"
#include "aeron_archive_client/Challenge.h"
#include "aeron_archive_client/RecordingSignalEvent.h"

using namespace aeron;
using namespace aeron::archive::client;

static aeron::controlled_poll_fragment_handler_t controlHandler(ControlResponsePoller &poller)
{
    return
        [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            return poller.onFragment(buffer, offset, length, header);
        };
}

ControlResponsePoller::ControlResponsePoller(std::shared_ptr<Subscription> subscription, int fragmentLimit) :
    m_fragmentAssembler(controlHandler(*this)),
    m_fragmentHandler(m_fragmentAssembler.handler()),
    m_subscription(std::move(subscription)),
    m_fragmentLimit(fragmentLimit)
{
}

ControlledPollAction ControlResponsePoller::onFragment(
    AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &)
{
    if (m_isPollComplete)
    {
        return ControlledPollAction::ABORT;
    }

    MessageHeader msgHeader(
        buffer.sbeData() + offset,
        static_cast<std::uint64_t>(length),
        MessageHeader::sbeSchemaVersion());

    const std::uint16_t schemaId = msgHeader.schemaId();
    if (schemaId != MessageHeader::sbeSchemaId())
    {
        throw ArchiveException(
            "expected schemaId=" + std::to_string(MessageHeader::sbeSchemaId()) +
            ", actual=" + std::to_string(schemaId),
            SOURCEINFO);
    }

    switch (msgHeader.templateId())
    {
        case ControlResponse::SBE_TEMPLATE_ID:
        {
            ControlResponse response(
                buffer.sbeData() + offset + MessageHeader::encodedLength(),
                static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
                msgHeader.blockLength(),
                msgHeader.version());

            m_controlSessionId = response.controlSessionId();
            m_correlationId = response.correlationId();
            m_relevantId = response.relevantId();
            m_version = response.version();

            ControlResponseCode::Value code = response.code();
            m_codeValue = code;
            m_isCodeError = ControlResponseCode::Value::ERROR == code;
            m_isCodeOk = ControlResponseCode::Value::OK == code;

            m_errorMessage = response.getErrorMessageAsString();

            m_isControlResponse = true;
            m_isPollComplete = true;

            return ControlledPollAction::BREAK;
        }

        case Challenge::SBE_TEMPLATE_ID:
        {
            Challenge challenge(
                buffer.sbeData() + offset + MessageHeader::encodedLength(),
                static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
                msgHeader.blockLength(),
                msgHeader.version());

            m_controlSessionId = challenge.controlSessionId();
            m_correlationId = challenge.correlationId();
            m_relevantId = aeron::NULL_VALUE;
            m_version = challenge.version();

            m_codeValue = ControlResponseCode::NULL_VALUE;
            m_isCodeError = false;
            m_isCodeOk = false;
            m_errorMessage = "";

            const std::uint32_t encodedChallengeLength = challenge.encodedChallengeLength();
            char *encodedBuffer = new char[encodedChallengeLength];
            challenge.getEncodedChallenge(encodedBuffer, encodedChallengeLength);

            m_encodedChallenge.first = encodedBuffer;
            m_encodedChallenge.second = encodedChallengeLength;

            m_isControlResponse = false;
            m_wasChallenged = true;
            m_isPollComplete = true;

            return ControlledPollAction::BREAK;
        }

        case RecordingSignalEvent::SBE_TEMPLATE_ID:
        {
            RecordingSignalEvent recordingSignalEvent(
                buffer.sbeData() + offset + MessageHeader::encodedLength(),
                static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
                msgHeader.blockLength(),
                msgHeader.version());

            m_controlSessionId = recordingSignalEvent.controlSessionId();
            m_correlationId = recordingSignalEvent.correlationId();
            m_recordingId = recordingSignalEvent.recordingId();
            m_subscriptionId = recordingSignalEvent.subscriptionId();
            m_position = recordingSignalEvent.position();
            m_recordingSignalCode = recordingSignalEvent.signalRaw();

            m_isRecordingSignal = true;
            m_isPollComplete = true;

            return ControlledPollAction::BREAK;
        }
    }


    return ControlledPollAction::CONTINUE;
}

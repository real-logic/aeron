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

#include "RecordingDescriptorPoller.h"
#include "ArchiveException.h"
#include "aeron_archive_client/MessageHeader.h"
#include "aeron_archive_client/ControlResponse.h"
#include "aeron_archive_client/RecordingDescriptor.h"

using namespace aeron;
using namespace aeron::archive::client;

static aeron::controlled_poll_fragment_handler_t controlHandler(RecordingDescriptorPoller& poller)
{
    return [&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        return poller.onFragment(buffer, offset, length, header);
    };
}

RecordingDescriptorPoller::RecordingDescriptorPoller(
    std::shared_ptr<Subscription> subscription,
    const exception_handler_t& errorHandler,
    std::int64_t controlSessionId,
    int fragmentLimit)
    :
    m_fragmentAssembler(controlHandler(*this)),
    m_fragmentHandler(m_fragmentAssembler.handler()),
    m_errorHandler(errorHandler),
    m_subscription(std::move(subscription)),
    m_controlSessionId(controlSessionId),
    m_fragmentLimit(fragmentLimit)
{
}

ControlledPollAction RecordingDescriptorPoller::onFragment(
    AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
{
    MessageHeader msgHeader(
        buffer.sbeData() + offset,
        static_cast<std::uint64_t>(length),
        MessageHeader::sbeSchemaVersion());

    const std::int16_t schemaId = msgHeader.schemaId();
    if (schemaId != MessageHeader::sbeSchemaId())
    {
        throw ArchiveException(
            "expected schemaId=" + std::to_string(MessageHeader::sbeSchemaId()) +
                ", actual=" + std::to_string(schemaId),
            SOURCEINFO);
    }

    const std::uint16_t templateId = msgHeader.templateId();
    switch (templateId)
    {
        case ControlResponse::sbeTemplateId():
        {
            ControlResponse response(
                buffer.sbeData() + offset + MessageHeader::encodedLength(),
                static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
                msgHeader.blockLength(),
                msgHeader.version());

            if (response.controlSessionId() == m_controlSessionId)
            {
                const ControlResponseCode::Value code = response.code();
                const std::int64_t correlationId = response.correlationId();

                if (ControlResponseCode::Value::RECORDING_UNKNOWN == code && correlationId == m_correlationId)
                {
                    m_isDispatchComplete = true;
                    return ControlledPollAction::BREAK;
                }

                if (ControlResponseCode::Value::ERROR == code)
                {
                    ArchiveException ex(
                        static_cast<std::int32_t>(response.relevantId()),
                        "response for correlationId=" + std::to_string(m_correlationId) +
                        ", error: " + response.errorMessage(),
                        SOURCEINFO);

                    if (correlationId == m_correlationId)
                    {
                        throw ArchiveException(ex);
                    }
                    else if (nullptr != m_errorHandler)
                    {
                        m_errorHandler(ex);
                    }
                }
            }
        }
        break;

        case RecordingDescriptor::sbeTemplateId():
        {
            RecordingDescriptor descriptor(
                buffer.sbeData() + offset + MessageHeader::encodedLength(),
                static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
                msgHeader.blockLength(),
                msgHeader.version());

            const std::int64_t correlationId = descriptor.correlationId();
            if (descriptor.controlSessionId() == m_controlSessionId && correlationId == m_correlationId)
            {
                m_consumer(
                    m_controlSessionId,
                    correlationId,
                    descriptor.recordingId(),
                    descriptor.startTimestamp(),
                    descriptor.stopTimestamp(),
                    descriptor.startPosition(),
                    descriptor.stopPosition(),
                    descriptor.initialTermId(),
                    descriptor.segmentFileLength(),
                    descriptor.termBufferLength(),
                    descriptor.mtuLength(),
                    descriptor.sessionId(),
                    descriptor.streamId(),
                    descriptor.strippedChannel(),
                    descriptor.originalChannel(),
                    descriptor.sourceIdentity());

                if (0 == --m_remainingRecordCount)
                {
                    m_isDispatchComplete = true;
                    return ControlledPollAction::BREAK;
                }
            }
        }
        break;

        default:
            break;
    }

    return ControlledPollAction::CONTINUE;
}

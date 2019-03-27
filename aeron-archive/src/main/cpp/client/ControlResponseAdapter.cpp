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

#include "ControlResponseAdapter.h"
#include "ArchiveException.h"
#include "aeron_archive_client/MessageHeader.h"
#include "aeron_archive_client/ControlResponse.h"
#include "aeron_archive_client/RecordingDescriptor.h"

using namespace aeron;
using namespace aeron::archive::client;

static aeron::fragment_handler_t fragmentHandler(ControlResponseAdapter& poller)
{
    return [&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        poller.onFragment(buffer, offset, length, header);
    };
}

ControlResponseAdapter::ControlResponseAdapter(
    const on_control_response_t& onResponse,
    const recording_descriptor_consumer_t& onRecordingDescriptor,
    std::shared_ptr<aeron::Subscription> subscription,
    int fragmentLimit) :
    m_fragmentHandler(fragmentHandler(*this)),
    m_subscription(std::move(subscription)),
    m_onResponse(onResponse),
    m_onRecordingDescriptor(onRecordingDescriptor),
    m_fragmentLimit(fragmentLimit)
{
}

void ControlResponseAdapter::onFragment(
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

            m_onResponse(
                response.controlSessionId(),
                response.correlationId(),
                response.relevantId(),
                response.code(),
                response.errorMessage());
            break;
        }

        case RecordingDescriptor::sbeTemplateId():
        {
            RecordingDescriptor descriptor(
                buffer.sbeData() + offset + MessageHeader::encodedLength(),
                static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
                msgHeader.blockLength(),
                msgHeader.version());

            m_onRecordingDescriptor(
                descriptor.controlSessionId(),
                descriptor.correlationId(),
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
            break;
        }

        default:
            break;
    }
}

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

#include "RecordingTransitionAdapter.h"
#include "ArchiveException.h"
#include "aeron_archive_client/MessageHeader.h"
#include "aeron_archive_client/ControlResponse.h"
#include "aeron_archive_client/RecordingTransition.h"

using namespace aeron;
using namespace aeron::archive::client;

static aeron::fragment_handler_t fragmentHandler(RecordingTransitionAdapter& adapter)
{
    return [&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        adapter.onFragment(buffer, offset, length, header);
    };
}

RecordingTransitionAdapter::RecordingTransitionAdapter(
    const on_control_response_t &onResponse,
    const on_recording_transition_t &onRecordingTransition,
    std::shared_ptr<aeron::Subscription> subscription,
    std::int64_t controlSessionId,
    int fragmentLimit) :
    m_fragmentHandler(fragmentHandler(*this)),
    m_subscription(std::move(subscription)),
    m_onResponse(onResponse),
    m_onRecordingTransition(onRecordingTransition),
    m_controlSessionId(controlSessionId),
    m_fragmentLimit(fragmentLimit)
{
}

void RecordingTransitionAdapter::onFragment(
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
                m_onResponse(
                    response.controlSessionId(),
                    response.correlationId(),
                    response.relevantId(),
                    response.code(),
                    response.errorMessage());
            }
            break;
        }

        case RecordingTransition::sbeTemplateId():
        {
            RecordingTransition recordingTransition(
                buffer.sbeData() + offset + MessageHeader::encodedLength(),
                static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
                msgHeader.blockLength(),
                msgHeader.version());

            if (recordingTransition.controlSessionId() == m_controlSessionId)
            {
                m_onRecordingTransition(
                    recordingTransition.controlSessionId(),
                    recordingTransition.recordingId(),
                    recordingTransition.subscriptionId(),
                    recordingTransition.position(),
                    recordingTransition.transitionType());
            }
            break;
        }

        default:
            break;
    }
}

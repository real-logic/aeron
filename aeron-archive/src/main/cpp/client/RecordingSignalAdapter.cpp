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

#include "ArchiveException.h"
#include "aeron_archive_client/MessageHeader.h"
#include "aeron_archive_client/ControlResponse.h"
#include "aeron_archive_client/RecordingSignalEvent.h"
#include "RecordingSignalAdapter.h"

using namespace aeron;
using namespace aeron::archive::client;

static aeron::controlled_poll_fragment_handler_t controlHandler(RecordingSignalAdapter &adapter)
{
    return
        [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            return adapter.onFragment(buffer, offset, length, header);
        };
}

RecordingSignalAdapter::RecordingSignalAdapter(
    const on_control_response_t &onResponse,
    const on_recording_signal_t &onRecordingSignal,
    std::shared_ptr<aeron::Subscription> subscription,
    std::int64_t controlSessionId,
    int fragmentLimit) :
    m_fragmentAssembler(controlHandler(*this)),
    m_fragmentHandler(m_fragmentAssembler.handler()),
    m_subscription(std::move(subscription)),
    m_onResponse(onResponse),
    m_onRecordingSignal(onRecordingSignal),
    m_controlSessionId(controlSessionId),
    m_fragmentLimit(fragmentLimit)
{
}

ControlledPollAction RecordingSignalAdapter::onFragment(
    AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
{
    if (m_isAbort)
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

    const std::uint16_t templateId = msgHeader.templateId();
    if (ControlResponse::sbeTemplateId() == templateId)
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
    }
    else if (RecordingSignalEvent::sbeTemplateId() == templateId)
    {
        RecordingSignalEvent recordingSignalEvent(
            buffer.sbeData() + offset + MessageHeader::encodedLength(),
            static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
            msgHeader.blockLength(),
            msgHeader.version());

        if (recordingSignalEvent.controlSessionId() == m_controlSessionId)
        {
            m_onRecordingSignal(
                recordingSignalEvent.controlSessionId(),
                recordingSignalEvent.recordingId(),
                recordingSignalEvent.subscriptionId(),
                recordingSignalEvent.position(),
                recordingSignalEvent.signalRaw());

            m_isAbort = true;
            return ControlledPollAction::BREAK;
        }
    }

    return ControlledPollAction::CONTINUE;
}

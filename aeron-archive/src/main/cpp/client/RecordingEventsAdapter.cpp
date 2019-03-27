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

#include "RecordingEventsAdapter.h"
#include "ArchiveException.h"
#include "aeron_archive_client/MessageHeader.h"
#include "aeron_archive_client/RecordingStarted.h"
#include "aeron_archive_client/RecordingProgress.h"
#include "aeron_archive_client/RecordingStopped.h"

using namespace aeron;
using namespace aeron::archive::client;

static aeron::fragment_handler_t fragmentHandler(RecordingEventsAdapter& poller)
{
    return [&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        poller.onFragment(buffer, offset, length, header);
    };
}

RecordingEventsAdapter::RecordingEventsAdapter(
    const on_recording_start_t& onStart,
    const on_recording_event_t& onProgress,
    const on_recording_event_t& onStop,
    std::shared_ptr<aeron::Subscription> subscription,
    int fragmentLimit) :
    m_fragmentHandler(fragmentHandler(*this)),
    m_subscription(std::move(subscription)),
    m_onStart(onStart),
    m_onProgress(onProgress),
    m_onStop(onStop),
    m_fragmentLimit(fragmentLimit)
{
}

void RecordingEventsAdapter::onFragment(
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
        case RecordingStarted::sbeTemplateId():
        {
            RecordingStarted event(
                buffer.sbeData() + offset + MessageHeader::encodedLength(),
                static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
                msgHeader.blockLength(),
                msgHeader.version());

            m_onStart(
                event.recordingId(),
                event.startPosition(),
                event.sessionId(),
                event.streamId(),
                event.channel(),
                event.sourceIdentity());
        }
        break;

        case RecordingProgress::sbeTemplateId():
        {
            RecordingProgress event(
                buffer.sbeData() + offset + MessageHeader::encodedLength(),
                static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
                msgHeader.blockLength(),
                msgHeader.version());

            m_onProgress(
                event.recordingId(),
                event.startPosition(),
                event.position());
        }
        break;

        case RecordingStopped::sbeTemplateId():
        {
            RecordingStopped event(
                buffer.sbeData() + offset + MessageHeader::encodedLength(),
                static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
                msgHeader.blockLength(),
                msgHeader.version());

            m_onStop(
                event.recordingId(),
                event.startPosition(),
                event.stopPosition());
        }
        break;

        default:
            break;
    }
}

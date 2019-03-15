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

#include "RecordingEventsPoller.h"
#include "ArchiveException.h"
#include "aeron_archive_client/MessageHeader.h"
#include "aeron_archive_client/RecordingStarted.h"
#include "aeron_archive_client/RecordingProgress.h"
#include "aeron_archive_client/RecordingStopped.h"

using namespace aeron;
using namespace aeron::archive::client;

static aeron::fragment_handler_t fragmentHandler(RecordingEventsPoller& poller)
{
    return [&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        poller.onFragment(buffer, offset, length, header);
    };
}

RecordingEventsPoller::RecordingEventsPoller(std::shared_ptr<aeron::Subscription> subscription) :
    m_fragmentHandler(fragmentHandler(*this)),
    m_subscription(std::move(subscription))
{
}

void RecordingEventsPoller::onFragment(
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

            m_eventType = EventType::RECORDING_STARTED;
            m_recordingId = event.recordingId();
            m_recordingStartPosition = event.startPosition();
            m_recordingPosition = m_recordingStartPosition;
            m_recordingStopPosition = aeron::NULL_VALUE;
            m_pollComplete = true;
        }
        break;

        case RecordingProgress::sbeTemplateId():
        {
            RecordingProgress event(
                buffer.sbeData() + offset + MessageHeader::encodedLength(),
                static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
                msgHeader.blockLength(),
                msgHeader.version());

            m_eventType = EventType::RECORDING_PROGRESS;
            m_recordingId = event.recordingId();
            m_recordingStartPosition = event.startPosition();
            m_recordingPosition = event.position();
            m_recordingStopPosition = aeron::NULL_VALUE;
            m_pollComplete = true;
        }
        break;

        case RecordingStopped::sbeTemplateId():
        {
            RecordingStopped event(
                buffer.sbeData() + offset + MessageHeader::encodedLength(),
                static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
                msgHeader.blockLength(),
                msgHeader.version());

            m_eventType = EventType::RECORDING_STOPPED;
            m_recordingId = event.recordingId();
            m_recordingStartPosition = event.startPosition();
            m_recordingPosition = event.stopPosition();
            m_recordingStopPosition = m_recordingPosition;
            m_pollComplete = true;
        }
        break;

        default:
            break;
    }
}


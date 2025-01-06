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

#include "RecordingEventsPoller.h"
#include "ArchiveException.h"
#include "aeron_archive_client/MessageHeader.h"
#include "aeron_archive_client/RecordingStarted.h"
#include "aeron_archive_client/RecordingProgress.h"
#include "aeron_archive_client/RecordingStopped.h"

using namespace aeron;
using namespace aeron::archive::client;

static aeron::controlled_poll_fragment_handler_t controlHandler(RecordingEventsPoller &poller)
{
    return
        [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            return poller.onFragment(buffer, offset, length, header);
        };
}

RecordingEventsPoller::RecordingEventsPoller(std::shared_ptr<aeron::Subscription> subscription) :
    m_fragmentHandler(controlHandler(*this)),
    m_subscription(std::move(subscription))
{
}

ControlledPollAction RecordingEventsPoller::onFragment(
    AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
{
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
    if (RecordingStarted::sbeTemplateId() == templateId)
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
        m_isPollComplete = true;

        return ControlledPollAction::BREAK;
    }
    else if (RecordingProgress::sbeTemplateId() == templateId)
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
        m_isPollComplete = true;

        return ControlledPollAction::BREAK;
    }
    else if (RecordingStopped::sbeTemplateId() == templateId)
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
        m_isPollComplete = true;

        return ControlledPollAction::BREAK;
    }

    return ControlledPollAction::CONTINUE;
}

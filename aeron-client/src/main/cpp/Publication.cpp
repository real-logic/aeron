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

#include <utility>

#include "Publication.h"
#include "ClientConductor.h"
#include "concurrent/status/LocalSocketAddressStatus.h"

namespace aeron
{

Publication::Publication(
    ClientConductor &conductor,
    const std::string &channel,
    std::int64_t registrationId,
    std::int64_t originalRegistrationId,
    std::int32_t streamId,
    std::int32_t sessionId,
    UnsafeBufferPosition &publicationLimit,
    std::int32_t channelStatusId,
    std::shared_ptr<LogBuffers> logBuffers) :
    m_conductor(conductor),
    m_logMetaDataBuffer(logBuffers->atomicBuffer(LogBufferDescriptor::LOG_META_DATA_SECTION_INDEX)),
    m_channel(channel),
    m_registrationId(registrationId),
    m_originalRegistrationId(originalRegistrationId),
    m_maxPossiblePosition(static_cast<std::int64_t>(logBuffers->atomicBuffer(0).capacity()) << 31),
    m_streamId(streamId),
    m_sessionId(sessionId),
    m_initialTermId(LogBufferDescriptor::initialTermId(m_logMetaDataBuffer)),
    m_maxPayloadLength(LogBufferDescriptor::mtuLength(m_logMetaDataBuffer) - DataFrameHeader::LENGTH),
    m_maxMessageLength(FrameDescriptor::computeMaxMessageLength(logBuffers->atomicBuffer(0).capacity())),
    m_positionBitsToShift(util::BitUtil::numberOfTrailingZeroes(logBuffers->atomicBuffer(0).capacity())),
    m_publicationLimit(publicationLimit),
    m_channelStatusId(channelStatusId),
    m_logBuffers(std::move(logBuffers)),
    m_headerWriter(LogBufferDescriptor::defaultFrameHeader(m_logMetaDataBuffer))
{
}

Publication::~Publication()
{
    m_isClosed.store(true, std::memory_order_release);
    m_conductor.releasePublication(m_registrationId);
}

std::int64_t Publication::addDestination(const std::string &endpointChannel)
{
    if (isClosed())
    {
        throw util::IllegalStateException(std::string("Publication is closed"), SOURCEINFO);
    }

    return m_conductor.addDestination(m_originalRegistrationId, endpointChannel);
}

std::int64_t Publication::removeDestination(const std::string &endpointChannel)
{
    if (isClosed())
    {
        throw util::IllegalStateException(std::string("Publication is closed"), SOURCEINFO);
    }

    return m_conductor.removeDestination(m_originalRegistrationId, endpointChannel);
}

bool Publication::findDestinationResponse(std::int64_t correlationId)
{
    return m_conductor.findDestinationResponse(correlationId);
}

std::int64_t Publication::channelStatus() const
{
    if (isClosed())
    {
        return ChannelEndpointStatus::NO_ID_ALLOCATED;
    }

    return m_conductor.channelStatus(m_channelStatusId);
}

std::vector<std::string> Publication::localSocketAddresses() const
{
    return LocalSocketAddressStatus::findAddresses(m_conductor.countersReader(), channelStatus(), channelStatusId());
}

}

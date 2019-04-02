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

#include "ExclusivePublication.h"
#include "ClientConductor.h"

namespace aeron {

ExclusivePublication::ExclusivePublication(
    ClientConductor &conductor,
    const std::string &channel,
    std::int64_t registrationId,
    std::int64_t originalRegistrationId,
    std::int32_t streamId,
    std::int32_t sessionId,
    UnsafeBufferPosition& publicationLimit,
    std::int32_t channelStatusId,
    std::shared_ptr<LogBuffers> logBuffers)
    :
    m_conductor(conductor),
    m_logMetaDataBuffer(logBuffers->atomicBuffer(LogBufferDescriptor::LOG_META_DATA_SECTION_INDEX)),
    m_channel(channel),
    m_registrationId(registrationId),
    m_originalRegistrationId(originalRegistrationId),
    m_maxPossiblePosition(static_cast<int64_t>(logBuffers->atomicBuffer(0).capacity()) << 31),
    m_streamId(streamId),
    m_sessionId(sessionId),
    m_initialTermId(LogBufferDescriptor::initialTermId(m_logMetaDataBuffer)),
    m_maxPayloadLength(LogBufferDescriptor::mtuLength(m_logMetaDataBuffer) - DataFrameHeader::LENGTH),
    m_maxMessageLength(FrameDescriptor::computeMaxMessageLength(logBuffers->atomicBuffer(0).capacity())),
    m_positionBitsToShift(util::BitUtil::numberOfTrailingZeroes(logBuffers->atomicBuffer(0).capacity())),
    m_activePartitionIndex(LogBufferDescriptor::indexByTermCount(LogBufferDescriptor::activeTermCount(m_logMetaDataBuffer))),
    m_publicationLimit(publicationLimit),
    m_channelStatusId(channelStatusId),
    m_logBuffers(std::move(logBuffers)),
    m_headerWriter(LogBufferDescriptor::defaultFrameHeader(m_logMetaDataBuffer))
{
    for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
    {
        /*
         * perhaps allow copy-construction and be able to move appenders and AtomicBuffers directly into Publication for
         * locality.
         */
        m_appenders[i] = std::unique_ptr<ExclusiveTermAppender>(new ExclusiveTermAppender(
            m_logBuffers->atomicBuffer(i),
            m_logBuffers->atomicBuffer(LogBufferDescriptor::LOG_META_DATA_SECTION_INDEX),
            i));
    }

    const std::int64_t rawTail = m_appenders[m_activePartitionIndex]->rawTail();
    m_termId = LogBufferDescriptor::termId(rawTail);
    m_termOffset = LogBufferDescriptor::termOffset(rawTail, m_logBuffers->atomicBuffer(0).capacity());
    m_termBeginPosition = LogBufferDescriptor::computeTermBeginPosition(m_termId, m_positionBitsToShift, m_initialTermId);
}

ExclusivePublication::~ExclusivePublication()
{
    m_conductor.releaseExclusivePublication(m_registrationId);
}

void ExclusivePublication::addDestination(const std::string& endpointChannel)
{
    if (isClosed())
    {
        throw util::IllegalStateException(std::string("Publication is closed"), SOURCEINFO);
    }

    m_conductor.addDestination(m_originalRegistrationId, endpointChannel);
}

void ExclusivePublication::removeDestination(const std::string& endpointChannel)
{
    if (isClosed())
    {
        throw util::IllegalStateException(std::string("Publication is closed"), SOURCEINFO);
    }

    m_conductor.removeDestination(m_originalRegistrationId, endpointChannel);
}

std::int64_t ExclusivePublication::channelStatus()
{
    if (isClosed())
    {
        return ChannelEndpointStatus::NO_ID_ALLOCATED;
    }

    return m_conductor.channelStatus(m_channelStatusId);
}

}

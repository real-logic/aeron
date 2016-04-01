/*
 * Copyright 2016 Real Logic Ltd.
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

#ifndef AERON_PUBLICATIONIMAGE_H
#define AERON_PUBLICATIONIMAGE_H

#include <cstdint>
#include <concurrent/AtomicBuffer.h>
#include <util/MacroUtil.h>
#include "buffer/MappedRawLog.h"
#include "media/InetAddress.h"

namespace aeron { namespace driver {

using namespace aeron::concurrent;
using namespace aeron::driver::buffer;
using namespace aeron::driver::media;

enum PublicationImageStatus
{
    INIT, ACTIVE, INACTIVE, LINGER
};

class PublicationImage
{
public:

    typedef std::shared_ptr<PublicationImage> ptr_t;

    PublicationImage(
        const int64_t correlationId,
        const int64_t imageLivenessTimeoutNs,
        const int32_t sessionId,
        const int32_t streamId,
        const int32_t positionBitsToShift,
        const int32_t termLengthMask,
        const int32_t initialTermId,
        const int32_t currentWindowLength,
        const int32_t currentGain,
        std::unique_ptr<MappedRawLog> rawLog,
        std::shared_ptr<InetAddress> sourceAddress,
        std::shared_ptr<InetAddress> controlAddress,
        std::shared_ptr<ReceiveChannelEndpoint> channelEndpoint
    )
        : m_correlationId(correlationId), m_imageLivenessTimeoutNs(imageLivenessTimeoutNs),
        m_sessionId(sessionId), m_streamId(streamId), m_positionBitsToShift(positionBitsToShift),
        m_termLengthMask(termLengthMask), m_initialTermId(initialTermId),
        m_currentWindowLength(currentWindowLength), m_currentGain(currentGain), m_rawLog(std::move(rawLog)),
        m_sourceAddress(sourceAddress), m_controlAddress(controlAddress), m_channelEndpoint(channelEndpoint)
    { }

    virtual ~PublicationImage(){}

    inline COND_MOCK_VIRTUAL std::int32_t sessionId()
    {
        return 0;
    }

    inline COND_MOCK_VIRTUAL std::int32_t streamId()
    {
        return 0;
    }

    inline COND_MOCK_VIRTUAL std::int32_t insertPacket(
        std::int32_t termId, std::int32_t termOffset, AtomicBuffer& buffer, std::int32_t length)
    {
        return 0;
    }

    inline COND_MOCK_VIRTUAL void ifActiveGoInactive()
    {
    }

    inline COND_MOCK_VIRTUAL void status(PublicationImageStatus status)
    {
        atomic::putValueVolatile(&m_status, status);
    }

private:

    // -- Cache-line padding

    std::int64_t m_timeOfLastStatusChange = 0;
    std::int64_t m_rebuildPosition = 0;

    volatile std::int64_t m_beginLossChange = -1;
    volatile std::int64_t m_endLossChange = -1;

    std::int32_t m_lossTermId = 0;
    std::int32_t m_lossTermOffset = 0;
    std::int32_t m_lossLength = 0;

    // -- Cache-line padding

    std::int64_t m_lastPacketTimestamp = 0;
    std::int64_t m_lastStatusMessageTimestamp = 0;
    std::int64_t m_lastStatusMessagePosition = 0;
    std::int64_t m_lastChangeNumber = -1;

    // -- Cache-line padding

    volatile std::int64_t newStatusMessagePosition;
    volatile PublicationImageStatus m_status = PublicationImageStatus::INIT;

    // -- Cache-line padding

    const std::int64_t m_correlationId;
    const std::int64_t m_imageLivenessTimeoutNs;
    const std::int32_t m_sessionId;
    const std::int32_t m_streamId;
    const std::int32_t m_positionBitsToShift;
    const std::int32_t m_termLengthMask;
    const std::int32_t m_initialTermId;
    const std::int32_t m_currentWindowLength;
    const std::int32_t m_currentGain;

    std::unique_ptr<MappedRawLog> m_rawLog;
    std::shared_ptr<InetAddress> m_sourceAddress;
    std::shared_ptr<InetAddress> m_controlAddress;
    std::shared_ptr<ReceiveChannelEndpoint> m_channelEndpoint;
};

}};

#endif //AERON_PUBLICATIONIMAGE_H

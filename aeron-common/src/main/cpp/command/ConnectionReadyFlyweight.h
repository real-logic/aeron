#ifndef INCLUDED_AERON_COMMAND_CONNECTIONREADYFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_CONNECTIONREADYFLYWEIGHT__

#include <cstdint>
#include <common/Flyweight.h>
#include "ReadyFlyweight.h"

namespace aeron { namespace common { namespace command {

/**
* Message to denote that new buffers have been added for a subscription.
*
* @see ControlProtocolEvents
*
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                         Correlation ID                        |
* |                                                               |
* +---------------------------------------------------------------+
* |                        Joining Position                       |
* |                                                               |
* +---------------------------------------------------------------+
* |                          Session ID                           |
* +---------------------------------------------------------------+
* |                           Stream ID                           |
* +---------------------------------------------------------------+
* |                           Term ID                             |
* +---------------------------------------------------------------+
* |                   Position Indicators Count                   |
* +---------------------------------------------------------------+
* |                          File Offset 0                        |
* +---------------------------------------------------------------+
* |                          File Offset 1                        |
* +---------------------------------------------------------------+
* |                          File Offset 2                        |
* +---------------------------------------------------------------+
* |                          File Offset 3                        |
* +---------------------------------------------------------------+
* |                          File Offset 4                        |
* +---------------------------------------------------------------+
* |                          File Offset 5                        |
* +---------------------------------------------------------------+
* |                             Length 0                          |
* +---------------------------------------------------------------+
* |                             Length 1                          |
* +---------------------------------------------------------------+
* |                             Length 2                          |
* +---------------------------------------------------------------+
* |                             Length 3                          |
* +---------------------------------------------------------------+
* |                             Length 4                          |
* +---------------------------------------------------------------+
* |                             Length 5                          |
* +---------------------------------------------------------------+
* |                          Location 1 Start                     |
* +---------------------------------------------------------------+
* |                          Location 2 Start                     |
* +---------------------------------------------------------------+
* |                          Location 3 Start                     |
* +---------------------------------------------------------------+
* |                          Location 4 Start                     |
* +---------------------------------------------------------------+
* |                          Location 5 Start                     |
* +---------------------------------------------------------------+
* |                     Source Information Start                  |
* +---------------------------------------------------------------+
* |                           Channel Start                       |
* +---------------------------------------------------------------+
* |                           Channel End                         |
* +---------------------------------------------------------------+
* |                            Location 0                       ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                            Location 1                       ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                            Location 2                       ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                            Location 3                       ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                            Location 4                       ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                            Location 5                       ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                            Channel                          ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                     Position Indicator Id 0                 ...
* +---------------------------------------------------------------+
* |                         Registration Id 0                   ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                     Position Indicator Id 1                 ...
* +---------------------------------------------------------------+
* |                         Registration Id 1                   ...
* |                                                             ...
* +---------------------------------------------------------------+
* |                                                             ...
* Up to "Position Indicators Count" entries of this form
*/


#pragma pack(push)
#pragma pack(4)
struct ConnectionReadyDefn
{
    static const std::int32_t NUM_FILES = 6;

    std::int64_t correlationId;
    std::int64_t joiningPosition;
    std::int32_t sessionId;
    std::int32_t streamId;
    std::int32_t termId;
    std::int32_t positionIndicatorsCount;
    std::int32_t fileOffset[NUM_FILES];
    std::int32_t length[NUM_FILES];
    std::int32_t locationStart[NUM_FILES];
    std::int32_t sourceInformationStart;
    std::int32_t channelStart;
    std::int32_t channelEnd;
    std::int32_t variableDataStart;
};
#pragma pack(pop)

class ConnectionReadyFlyweight : public common::Flyweight<ConnectionReadyDefn>,
                                 public ReadyFlyweight<ConnectionReadyFlyweight>
{
public:
    inline ConnectionReadyFlyweight (concurrent::AtomicBuffer& buffer, size_t offset)
        : common::Flyweight<ConnectionReadyDefn>(buffer, offset)
    {
    }

    inline std::int32_t bufferOffset(std::int32_t index) const
    {
        return m_struct.fileOffset[index];
    }

    inline ConnectionReadyFlyweight& bufferOffset(std::int32_t index, std::int32_t value)
    {
        m_struct.fileOffset[index] = value;
        return *this;
    }

    inline std::int32_t bufferLength(std::int32_t index) const
    {
        return m_struct.length[index];
    }

    inline ConnectionReadyFlyweight& bufferLength(std::int32_t index, std::int32_t value)
    {
        m_struct.length[index] = value;
        return *this;
    }

    inline std::string location(std::int32_t index) const
    {
        return "";
    }

    inline ConnectionReadyFlyweight& location(std::int32_t index, const std::string &value)
    {
        return *this;
    }

    inline std::int64_t correlationId() const
    {
        return m_struct.correlationId;
    }

    inline ConnectionReadyFlyweight& correlationId(std::int64_t value)
    {
        m_struct.correlationId = value;
        return *this;
    }

    inline std::int64_t joiningPosition() const
    {
        return m_struct.joiningPosition;
    }

    inline ConnectionReadyFlyweight& joiningPosition(std::int64_t value)
    {
        m_struct.joiningPosition = value;
        return *this;
    }

    inline std::int32_t sessionId() const
    {
        return m_struct.sessionId;
    }

    inline ConnectionReadyFlyweight& sessionId(std::int32_t value)
    {
        m_struct.sessionId = value;
        return *this;
    }

    inline std::int32_t streamId() const
    {
        return m_struct.streamId;
    }

    inline ConnectionReadyFlyweight& streamId(std::int32_t value)
    {
        m_struct.streamId = value;
        return *this;
    }

    inline std::int32_t termId() const
    {
        return m_struct.termId;
    }

    inline ConnectionReadyFlyweight& termId(std::int32_t value)
    {
        m_struct.termId = value;
        return *this;
    }

    inline std::int32_t positionIndicatorsCount() const
    {
        return m_struct.positionIndicatorsCount;
    }

    inline ConnectionReadyFlyweight& positionIndicatorsCount(std::int32_t value)
    {
        m_struct.positionIndicatorsCount = value;
        return *this;
    }

    inline std::int32_t sourceInformationStart() const
    {
        return m_struct.sourceInformationStart;
    }

    inline ConnectionReadyFlyweight& sourceInformationStart(std::int32_t value)
    {
        m_struct.sourceInformationStart = value;
        return *this;
    }

    inline std::int32_t channelStart() const
    {
        return m_struct.channelStart;
    }

    inline ConnectionReadyFlyweight& channelStart(std::int32_t value)
    {
        m_struct.channelStart = value;
        return *this;
    }

    inline std::int32_t channelEnd() const
    {
        return m_struct.channelEnd;
    }

    inline ConnectionReadyFlyweight& channelEnd(std::int32_t value)
    {
        m_struct.channelEnd = value;
        return *this;
    }
};

}}};

#endif
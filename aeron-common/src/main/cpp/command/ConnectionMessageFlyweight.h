#ifndef INCLUDED_AERON_COMMAND_CONNECTIONMESSAGEFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_CONNECTIONMESSAGEFLYWEIGHT__

#include <cstdint>
#include <common/Flyweight.h>

namespace aeron { namespace common { namespace command {

/**
* Control message flyweight for any message that needs to represent a connection
*
* <p>
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                        Correlation ID                         |
* |                                                               |
* +---------------------------------------------------------------+
* |                          Session ID                           |
* +---------------------------------------------------------------+
* |                          Stream ID                            |
* +---------------------------------------------------------------+
* |      Channel   Length       |   Channel                     ...
* |                                                             ...
* +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct ConnectionMessageDefn
{
    std::int64_t correlationId;
    std::int32_t sessionId;
    std::int32_t streamId;
    struct
    {
        std::int32_t channelLength;
        std::int8_t  channelData[1];
    } channel;
};
#pragma pack(pop)


class ConnectionMessageFlyweight : public common::Flyweight<ConnectionMessageDefn>
{
public:
    inline ConnectionMessageFlyweight (concurrent::AtomicBuffer& buffer, size_t offset)
            : common::Flyweight<ConnectionMessageDefn>(buffer, offset)
    {
    }

    inline std::int64_t correlationId() const
    {
        return m_struct.correlationId;
    }

    inline ConnectionMessageFlyweight& correlationId(std::int64_t correlationId)
    {
        m_struct.correlationId = correlationId;
        return *this;
    }

    inline std::int32_t sessionId() const
    {
        return m_struct.sessionId;
    }

    inline ConnectionMessageFlyweight& sessionId(std::int32_t sessionId)
    {
        m_struct.sessionId = sessionId;
        return *this;
    }

    inline std::int32_t streamId() const
    {
        return m_struct.streamId;
    }

    inline ConnectionMessageFlyweight& streamId(std::int32_t streamId)
    {
        m_struct.streamId = streamId;
        return *this;
    }

    inline std::string channel()
    {
        return stringGet(offsetof(ConnectionMessageDefn, channel));
    }

    inline ConnectionMessageFlyweight& channel(const std::string& chan)
    {
        stringPut(offsetof(ConnectionMessageDefn, channel), chan);
        return *this;
    }

    std::size_t length()
    {
        return offsetof(ConnectionMessageDefn, channel.channelData) + m_struct.channel.channelLength;
    }
};

}}};
#endif
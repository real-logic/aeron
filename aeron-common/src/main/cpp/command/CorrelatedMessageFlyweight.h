#ifndef INCLUDED_AERON_COMMAND_CORRELATEDMESSAGEFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_CORRELATEDMESSAGEFLYWEIGHT__

#include <cstdint>
#include <common/Flyweight.h>

namespace aeron { namespace common { namespace command {

/**
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                            Client ID                          |
* |                                                               |
* +---------------------------------------------------------------+
* |                         Correlation ID                        |
* |                                                               |
* +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct CorrelatedMessageDefn
{
    std::int64_t clientId;
    std::int64_t correlationId;
};
#pragma pack(pop)


class CorrelatedMessageFlyweight : public common::Flyweight<CorrelatedMessageDefn>
{
public:
    inline CorrelatedMessageFlyweight (concurrent::AtomicBuffer& buffer, size_t offset)
            : common::Flyweight<CorrelatedMessageDefn>(buffer, offset)
    {
    }

    inline std::int64_t correlationId() const
    {
        return m_struct.correlationId;
    }

    inline CorrelatedMessageFlyweight& correlationId(std::int64_t value)
    {
        m_struct.correlationId = value;
        return *this;
    }

    inline std::int64_t clientId() const
    {
        return m_struct.clientId;
    }

    inline CorrelatedMessageFlyweight& clientId(std::int64_t value)
    {
        m_struct.clientId = value;
        return *this;
    }
};

}}};
#endif
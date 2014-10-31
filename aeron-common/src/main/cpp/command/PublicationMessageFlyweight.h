#ifndef INCLUDED_AERON_COMMAND_PUBLICATIONMESSAGEFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_PUBLICATIONMESSAGEFLYWEIGHT__

#include <cstdint>
#include <common/Flyweight.h>

namespace aeron { namespace common { namespace command {

/**
* Control message for adding or removing a publication
*
* <p>
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                         Correlation ID                        |
* |                                                               |
* +---------------------------------------------------------------+
* |                          Session ID                           |
* +---------------------------------------------------------------+
* |                          Stream ID                            |
* +---------------------------------------------------------------+
* |      Channel Length         |           Channel             ...
* |                                                             ...
* +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct PublicationMessageDefn
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


class PublicationMessageFlyweight : public common::Flyweight<PublicationMessageDefn>
{
public:
    inline PublicationMessageFlyweight (concurrent::AtomicBuffer& buffer, size_t offset)
            : common::Flyweight<PublicationMessageDefn>(buffer, offset)
    {
    }

    inline std::int64_t correlationId() const
    {
        return m_struct.correlationId;
    }

    inline PublicationMessageFlyweight& correlationId(std::int64_t correlationId)
    {
        m_struct.correlationId = correlationId;
        return *this;
    }

    inline std::int32_t sessionId() const
    {
        return m_struct.sessionId;
    }

    inline PublicationMessageFlyweight& sessionId(std::int32_t sessionId)
    {
        m_struct.sessionId = sessionId;
        return *this;
    }

    inline std::int32_t streamId() const
    {
        return m_struct.streamId;
    }

    inline PublicationMessageFlyweight& streamId(std::int32_t streamId)
    {
        m_struct.streamId = streamId;
        return *this;
    }

    inline std::string channel()
    {
        return stringGet(offsetof(PublicationMessageDefn, channel));
    }

    inline PublicationMessageFlyweight& channel(const std::string& chan)
    {
        stringPut(offsetof(PublicationMessageDefn, channel), chan);
        return *this;
    }

    std::size_t length()
    {
        return offsetof(PublicationMessageDefn, channel.channelData) + m_struct.channel.channelLength;
    }
};

}}};
#endif
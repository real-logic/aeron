//
// Created by Michael Barker on 24/11/2015.
//

#ifndef INCLUDED_AERON_COMMAND_HEADERFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_HEADERFLYWEIGHT__

#include <cstdint>
#include <string>
#include <stddef.h>
#include <command/Flyweight.h>
#include <concurrent/AtomicBuffer.h>
#include <util/Index.h>

namespace aeron { namespace protocol {

/**
 * Flyweight for command header fields.
 */

#pragma pack(push)
#pragma pack(4)
struct HeaderDefn
{
    std::int32_t frameLength;
    std::int8_t version;
    std::int8_t flags;
    std::int16_t type;
};
#pragma pack(pop)


class HeaderFlyweight : public command::Flyweight<HeaderDefn>
{
public:
    typedef HeaderFlyweight this_t;

    HeaderFlyweight(concurrent::AtomicBuffer& buffer, std::int32_t offset)
    : command::Flyweight<HeaderDefn>(buffer, offset)
    {
    }

    inline std::int32_t frameLength() const
    {
        return m_struct.frameLength;
    }

    inline this_t& frameLength(std::int32_t value)
    {
        m_struct.frameLength = value;
        return *this;
    }

    inline std::int8_t version() const
    {
        return m_struct.version;
    }

    inline this_t& version(std::int8_t value)
    {
        m_struct.version = value;
        return *this;
    }

    inline std::int8_t flags() const
    {
        return m_struct.flags;
    }

    inline this_t& flags(std::int8_t value)
    {
        m_struct.flags = value;
        return *this;
    }

    inline std::int16_t type() const
    {
        return m_struct.type;
    }

    inline this_t& type(std::int16_t value)
    {
        m_struct.type = value;
        return *this;
    }
};

}}

#endif //INCLUDED_AERON_COMMAND_HEADERFLYWEIGHT__

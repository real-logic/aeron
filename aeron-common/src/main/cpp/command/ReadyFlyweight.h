#ifndef INCLUDED_AERON_COMMAND_READYFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_READYFLYWEIGHT__

#include <cstdint>
#include <string>

namespace aeron { namespace common { namespace command {

// interface
template <typename concrete_t>
struct ReadyFlyweight
{
    virtual std::int32_t bufferOffset(std::int32_t index) const = 0;
    virtual concrete_t& bufferOffset(std::int32_t index, std::int32_t value) = 0;
    virtual std::int32_t bufferLength(std::int32_t index) const = 0;
    virtual concrete_t&  bufferLength(std::int32_t index, std::int32_t value) = 0;
    virtual std::string location(std::int32_t index) const = 0;
    virtual concrete_t& location(std::int32_t index, const std::string& value) = 0;

    virtual ~ReadyFlyweight() {}
};

}}};
#endif
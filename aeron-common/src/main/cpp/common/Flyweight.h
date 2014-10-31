#ifndef INCLUDED_AERON_COMMON_FLYWEIGHT__
#define INCLUDED_AERON_COMMON_FLYWEIGHT__

#include <string>
#include <concurrent/AtomicBuffer.h>

namespace aeron { namespace common { namespace common {

template<typename struct_t>
class Flyweight
{
public:
    Flyweight (concurrent::AtomicBuffer& buffer, size_t offset)
        : m_struct(buffer.overlayStruct<struct_t>(offset)),
          m_buffer(buffer), m_baseOffset(offset)
    {
    }

protected:
    struct_t& m_struct;

    inline std::string stringGet(size_t offset)
    {
        return m_buffer.getStringUtf8(m_baseOffset + offset);
    }

    inline std::int32_t stringPut(int offset, const std::string& s)
    {
        return m_buffer.putStringUtf8(m_baseOffset + offset, s);
    }

    inline std::string stringGet(size_t offset, size_t size)
    {
        return m_buffer.getStringUtf8(m_baseOffset + offset, size);
    }

    template <typename struct_t2>
    inline struct_t2& overlayStruct (size_t offset)
    {
        return m_buffer.overlayStruct<struct_t2>(m_baseOffset + offset);
    }

private:
    concurrent::AtomicBuffer m_buffer;
    size_t m_baseOffset;
};

}}}

#endif
#include "AtomicBuffer.h"

namespace aeron { namespace common { namespace concurrent {

AtomicBuffer::AtomicBuffer(std::uint8_t *buffer, size_t length)
    : m_buffer (buffer), m_length(length)
{
}

}}}